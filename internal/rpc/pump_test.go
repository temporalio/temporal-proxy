package rpc_test

import (
	"errors"
	"io"
	"testing"
	"testing/synctest"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/temporalio/temporal-proxy/internal/rpc"
	"github.com/temporalio/temporal-proxy/pkg/testutil"
)

type (
	// frame is what the pump moves. The pump never looks inside the value its
	// allocator returns, so a test needs nothing more than something comparable.
	frame struct {
		payload string
	}

	// clientStream is the upstream side of a forwarded call: it hands back the
	// queued responses in order and then reports RecvErr, defaulting to the io.EOF a
	// completed call reports, while recording what the pump sent upstream.
	clientStream struct {
		testutil.ClientStream
		header  metadata.MD
		trailer metadata.MD
		queued  []frame
		sent    []frame
		closed  bool

		// park holds every receive until it is closed, which keeps this direction
		// from reporting anything while the other one decides the call's outcome.
		park chan struct{}
	}

	// serverStream is the caller side of a forwarded call: it hands back the queued
	// requests in order and then reports RecvErr, defaulting to the io.EOF a
	// half-close reports, while recording what the pump sent back.
	serverStream struct {
		testutil.ServerStream
		header  metadata.MD
		trailer metadata.MD
		queued  []frame
		sent    []frame

		// headerSentAfter is how many response messages had already reached the
		// caller when the header was relayed. gRPC flushes the header on the first
		// send, so anything but zero is a header the caller never sees.
		headerSentAfter int
	}
)

func TestForwardRelaysBothDirections(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		requests := []frame{{payload: "orders"}, {payload: "payments"}}
		responses := []frame{{payload: "page-1"}, {payload: "page-2"}}

		header := metadata.Pairs("x-upstream", "cloud")
		trailer := metadata.Pairs("x-upstream-retry", "0")

		// The upstream is held until the request direction has finished, which fixes
		// the order the two directions complete in. Left to race, whichever finished
		// first would decide whether the half-close had happened yet.
		release := make(chan struct{})
		cs := &clientStream{header: header, trailer: trailer, queued: responses, park: release}
		ss := &serverStream{queued: requests}

		done := make(chan error, 1)
		go func() { done <- forward(t, cs, ss) }()

		synctest.Wait()
		requireMessages(t, requests, cs.sent)
		require.True(t, cs.closed, "expected the caller's half-close to close the upstream's send side")

		close(release)
		synctest.Wait()
		require.NoError(t, <-done)
		requireMessages(t, responses, ss.sent)
		require.Equal(t, header, ss.header, "expected the upstream's response header to reach the caller")
		require.Zero(t, ss.headerSentAfter, "expected the header to be relayed before the first response")
		require.Equal(t, trailer, ss.trailer, "expected the upstream's trailer to reach the caller")
	})
}

func TestForwardDrainsResponsesAfterHalfClose(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		responses := []frame{{payload: "page-1"}}

		// The caller has half-closed (no queued requests) while the upstream is still
		// working, which is the ordinary shape of a client-streaming call.
		release := make(chan struct{})
		cs := &clientStream{queued: responses, park: release}
		ss := &serverStream{}

		done := make(chan error, 1)
		go func() { done <- forward(t, cs, ss) }()

		// Half-closing the request side must not end the call: the caller is still
		// owed every response, the trailer, and the upstream's status.
		synctest.Wait()
		require.Empty(t, done, "expected Forward to wait on the upstream after the caller half-closed")

		close(release)
		synctest.Wait()
		require.NoError(t, <-done)
		requireMessages(t, responses, ss.sent)
	})
}

func TestForwardReportsRequestFailures(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		cs   *clientStream
		ss   *serverStream
		want string
	}{
		{
			name: "a failed receive from the caller is reported",
			ss:   &serverStream{ServerStream: testutil.ServerStream{RecvErr: errors.New("recv failed")}},
			want: "recv failed",
		},
		{
			name: "a failed send upstream is reported",
			cs:   &clientStream{ClientStream: testutil.ClientStream{SendErr: errors.New("send failed")}},
			ss:   &serverStream{queued: []frame{{payload: "orders"}}},
			want: "send failed",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cs, ss := tt.cs, tt.ss
			if cs == nil {
				cs = &clientStream{}
			}

			// The response direction is parked so that the request failure is the only
			// outcome Forward can select, and released afterwards so its goroutine ends
			// with the test rather than outliving it.
			cs.park = make(chan struct{})
			t.Cleanup(func() { close(cs.park) })

			err := forward(t, cs, ss)
			require.Equal(t, codes.Internal, status.Code(err))
			require.ErrorContains(t, err, "forwarding the request stream failed")
			require.ErrorContains(t, err, tt.want)
		})
	}
}

func TestForwardReportsResponseFailures(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		cs   *clientStream
		ss   *serverStream
		code codes.Code
		want string
	}{
		{
			name: "a failed upstream header is reported",
			cs:   &clientStream{ClientStream: testutil.ClientStream{HeaderErr: errors.New("header failed")}},
			code: codes.Internal,
			want: "header failed",
		},
		{
			name: "a header the caller refuses is reported",
			ss:   &serverStream{ServerStream: testutil.ServerStream{HeaderErr: errors.New("send header failed")}},
			code: codes.Internal,
			want: "send header failed",
		},
		{
			name: "a failed upstream receive is reported",
			cs:   &clientStream{ClientStream: testutil.ClientStream{RecvErr: errors.New("recv failed")}},
			code: codes.Internal,
			want: "recv failed",
		},
		{
			// The upstream's own status is what the caller is owed, so it passes
			// through with its code rather than being flattened to Internal.
			name: "an upstream status is forwarded verbatim",
			cs: &clientStream{
				ClientStream: testutil.ClientStream{RecvErr: status.Error(codes.NotFound, "namespace not found")},
			},
			code: codes.NotFound,
			want: "namespace not found",
		},
		{
			name: "a response the caller refuses is reported",
			cs:   &clientStream{queued: []frame{{payload: "page-1"}}},
			ss:   &serverStream{ServerStream: testutil.ServerStream{SendErr: errors.New("send failed")}},
			code: codes.Internal,
			want: "send failed",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// The request direction half-closes at once, which Forward treats as the
			// call continuing, so the response failure is the outcome either ordering
			// arrives at.
			err := forward(t, tt.cs, tt.ss)
			require.Equal(t, tt.code, status.Code(err))
			require.ErrorContains(t, err, tt.want)
		})
	}
}

// Header returns the configured header, or HeaderErr when one is set.
func (s *clientStream) Header() (metadata.MD, error) {
	if err := s.HeaderErr; err != nil {
		return nil, err
	}

	return s.header, nil
}

// Trailer returns the configured trailer.
func (s *clientStream) Trailer() metadata.MD { return s.trailer }

// CloseSend records the half-close the pump performs on the upstream.
func (s *clientStream) CloseSend() error {
	s.closed = true
	return nil
}

// SendMsg reports SendErr, and otherwise records a request the pump forwarded
// upstream.
func (s *clientStream) SendMsg(m any) error {
	if err := s.SendErr; err != nil {
		return err
	}

	s.sent = append(s.sent, *m.(*frame))

	return nil
}

// RecvMsg fills m with the next queued response, waiting on park when one is
// configured.
func (s *clientStream) RecvMsg(m any) error {
	if s.park != nil {
		<-s.park
	}

	return next(&s.queued, m, s.RecvErr)
}

// SendHeader reports HeaderErr, and otherwise records the header the pump relayed
// along with how many responses had already gone out ahead of it.
func (s *serverStream) SendHeader(md metadata.MD) error {
	if err := s.HeaderErr; err != nil {
		return err
	}

	s.header = md
	s.headerSentAfter = len(s.sent)

	return nil
}

// SetTrailer records the trailer the pump relayed.
func (s *serverStream) SetTrailer(md metadata.MD) { s.trailer = md }

// SendMsg reports SendErr, and otherwise records a response the pump relayed to
// the caller.
func (s *serverStream) SendMsg(m any) error {
	if err := s.SendErr; err != nil {
		return err
	}

	s.sent = append(s.sent, *m.(*frame))

	return nil
}

// RecvMsg fills m with the next queued request.
func (s *serverStream) RecvMsg(m any) error {
	return next(&s.queued, m, s.RecvErr)
}

// forward runs one call through a Pump over the given streams. A nil stream stands
// in for one the case does not configure, which then succeeds and carries no
// messages.
func forward(t *testing.T, cs *clientStream, ss *serverStream) error {
	t.Helper()

	if cs == nil {
		cs = &clientStream{}
	}

	if ss == nil {
		ss = &serverStream{}
	}

	alloc := func() any { return &frame{} }

	return rpc.NewPump(cs, ss).Forward(alloc, alloc)
}

// next pops the head of queue into m, reporting drained (io.EOF unless a test
// says otherwise) once the queue is empty. It fills m rather than replacing it
// because the pump owns that value, having just allocated it.
func next(queue *[]frame, m any, drained error) error {
	if len(*queue) == 0 {
		if drained != nil {
			return drained
		}

		return io.EOF
	}

	*m.(*frame) = (*queue)[0]
	*queue = (*queue)[1:]

	return nil
}

// requireMessages asserts got holds exactly want, in order, so a pump that
// dropped or reordered a frame fails.
func requireMessages(t *testing.T, want, got []frame) {
	t.Helper()

	require.Equal(t, want, got)
}
