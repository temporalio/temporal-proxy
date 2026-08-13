// package router (not router_test): these tests exercise the unexported frame
// type the interceptor buffers and replays.
package router

import (
	"context"
	"errors"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/temporalio/temporal-proxy/internal/transport/meta"
)

type (
	// fakeStream is a grpc.ServerStream whose RecvMsg replays a queued script, so
	// a test can hand the interceptor a first frame, a half-close, or a transport
	// error without standing up a server.
	fakeStream struct {
		ctx   context.Context
		recv  []recvStep
		next  int
		calls int
	}

	recvStep struct {
		payload []byte
		err     error
	}

	// peekReflector records what it was asked to reflect and returns a fixed
	// namespace. The interceptor calls it inline, so no locking is needed.
	peekReflector struct {
		ns      string
		calls   int
		method  string
		payload []byte
	}

	// allowlistFunc adapts a predicate to services.Allowlist.
	allowlistFunc func(string) bool
)

func TestPeekInterceptorAttachesTargetAndReplaysFirstFrame(t *testing.T) {
	t.Parallel()

	first, err := proto.Marshal(&grpc_health_v1.HealthCheckRequest{Service: "abc"})
	require.NoError(t, err)

	reflector := &peekReflector{ns: "orders"}
	ss := &fakeStream{ctx: t.Context(), recv: []recvStep{{payload: first}}}

	var (
		gotTarget  meta.Target
		gotPayload []byte
		gotErr     error
	)

	err = PeekInterceptor(reflector, allowlistFunc(func(string) bool { return true }))(
		nil,
		ss,
		&grpc.StreamServerInfo{FullMethod: "/test.v1.Echo/Ping"},
		func(_ any, stream grpc.ServerStream) error {
			gotTarget = meta.TargetFrom(stream.Context())

			f := &frame{}
			gotErr = stream.RecvMsg(f)
			gotPayload = f.payload

			return nil
		},
	)
	require.NoError(t, err)

	// The Target names the method the stream carried and the namespace the
	// Reflector read out of its first message.
	require.Equal(t, meta.Target{FullName: "/test.v1.Echo/Ping", Namespace: "orders"}, gotTarget)

	// The frame the interceptor consumed is handed back to the handler verbatim.
	require.NoError(t, gotErr)
	require.Equal(t, first, gotPayload)

	// The Reflector saw the method and the raw first-frame bytes.
	require.Equal(t, 1, reflector.calls)
	require.Equal(t, "/test.v1.Echo/Ping", reflector.method)
	require.Equal(t, first, reflector.payload)
}

func TestPeekInterceptorHalfCloseSkipsReflector(t *testing.T) {
	t.Parallel()

	// A client that half-closes without sending a message has no payload to read a
	// namespace from, so the Reflector is never asked and the stream still runs:
	// a client-streaming call sending zero messages is legal.
	reflector := &peekReflector{ns: "never-used"}
	ss := &fakeStream{ctx: t.Context(), recv: []recvStep{{err: io.EOF}}}

	var (
		ran       bool
		gotTarget meta.Target
		gotErr    error
	)

	err := PeekInterceptor(reflector, allowlistFunc(func(string) bool { return true }))(
		nil,
		ss,
		&grpc.StreamServerInfo{FullMethod: "/test.v1.Count/Sum"},
		func(_ any, stream grpc.ServerStream) error {
			ran = true
			gotTarget = meta.TargetFrom(stream.Context())
			gotErr = stream.RecvMsg(&frame{})

			return nil
		},
	)
	require.NoError(t, err)

	require.True(t, ran, "the handler must still run after a half-close")
	require.Equal(t, meta.Target{FullName: "/test.v1.Count/Sum"}, gotTarget)
	require.Zero(t, reflector.calls, "there is no payload to reflect on")

	// The handler observes the half-close itself, so it can close the upstream
	// send side rather than replaying a frame that never existed.
	require.ErrorIs(t, gotErr, io.EOF)
}

func TestPeekInterceptorSkipsServiceItDoesNotForward(t *testing.T) {
	t.Parallel()

	// Only forwarded services are peeked. A service registered on this server
	// reads its own first message into its own type, so consuming it here would
	// take it away. The stream is passed through untouched and the rejection is
	// left to the handler, so an unauthenticated caller still cannot learn which
	// services exist by watching where the peek happens.
	first, err := proto.Marshal(&grpc_health_v1.HealthCheckRequest{Service: "abc"})
	require.NoError(t, err)

	reflector := &peekReflector{ns: "never-used"}
	ss := &fakeStream{ctx: t.Context(), recv: []recvStep{{payload: first}}}

	var (
		ran        bool
		gotTarget  meta.Target
		gotPayload []byte
	)

	err = PeekInterceptor(reflector, allowlistFunc(func(string) bool { return false }))(
		nil,
		ss,
		&grpc.StreamServerInfo{FullMethod: "/grpc.health.v1.Health/Check"},
		func(_ any, stream grpc.ServerStream) error {
			ran = true
			gotTarget = meta.TargetFrom(stream.Context())

			f := &frame{}
			_ = stream.RecvMsg(f)
			gotPayload = f.payload

			return nil
		},
	)
	require.NoError(t, err)

	require.True(t, ran, "a denied service is still handled, just not peeked")
	require.Zero(t, reflector.calls)

	// The method still travels, because authentication runs for every stream and a
	// provider weighs the method being invoked. Only the payload is left alone, so
	// the namespace is unknown.
	require.Equal(t, meta.Target{FullName: "/grpc.health.v1.Health/Check"}, gotTarget)

	// The handler reads the first message itself: the interceptor never took it.
	require.Equal(t, 1, ss.calls, "the first message must be left for the handler to read")
	require.Equal(t, first, gotPayload)
}

func TestPeekInterceptorReportsUnexpectedReplayTarget(t *testing.T) {
	t.Parallel()

	// Only a service the router forwards is peeked, and the router reads frames, so
	// a destination of any other type cannot arise: it would take a service both
	// registered on this server and accepted by config.Services.Validate, which
	// admits only forwardable names. Reporting it beats decoding it. Decoding would
	// silently support a combination nothing produces, and a reader would have to
	// work out which one; this says so, and cannot panic the process, which matters
	// because the server installs no panic recovery.
	first, err := proto.Marshal(&grpc_health_v1.HealthCheckRequest{Service: "abc"})
	require.NoError(t, err)

	ss := &fakeStream{ctx: t.Context(), recv: []recvStep{{payload: first}}}

	var gotErr error

	err = PeekInterceptor(&peekReflector{}, allowlistFunc(func(string) bool { return true }))(
		nil,
		ss,
		&grpc.StreamServerInfo{FullMethod: "/test.v1.Echo/Ping"},
		func(_ any, stream grpc.ServerStream) error {
			gotErr = stream.RecvMsg(new(grpc_health_v1.HealthCheckRequest))

			return nil
		},
	)
	require.NoError(t, err)

	require.Error(t, gotErr)
	require.Equal(t, codes.Internal, status.Code(gotErr))
	require.Contains(t, status.Convert(gotErr).Message(), "frame")
}

func TestPeekInterceptorReportsFirstReadFailure(t *testing.T) {
	t.Parallel()

	// A read that fails for any reason other than a half-close aborts the stream,
	// and says which step failed: this is the only place that read happens now, so
	// a bare transport error would reach the caller as an unexplained Unknown.
	reflector := &peekReflector{}
	ss := &fakeStream{ctx: t.Context(), recv: []recvStep{{err: errors.New("boom")}}}

	ran := false
	err := PeekInterceptor(reflector, allowlistFunc(func(string) bool { return true }))(
		nil,
		ss,
		&grpc.StreamServerInfo{FullMethod: "/test.v1.Echo/Ping"},
		func(any, grpc.ServerStream) error {
			ran = true

			return nil
		},
	)

	require.Error(t, err)
	require.False(t, ran, "a stream whose first read failed must not be handled")
	require.Equal(t, codes.Internal, status.Code(err))
	require.Contains(t, status.Convert(err).Message(), "reading the first request failed")
	require.Zero(t, reflector.calls)
}

func (s *fakeStream) SetHeader(metadata.MD) error  { return nil }
func (s *fakeStream) SendHeader(metadata.MD) error { return nil }
func (s *fakeStream) SetTrailer(metadata.MD)       {}
func (s *fakeStream) Context() context.Context     { return s.ctx }
func (s *fakeStream) SendMsg(any) error            { return nil }

func (s *fakeStream) RecvMsg(m any) error {
	s.calls++
	if s.next >= len(s.recv) {
		return io.EOF
	}

	step := s.recv[s.next]
	s.next++

	if step.err != nil {
		return step.err
	}

	m.(*frame).payload = step.payload

	return nil
}

func (r *peekReflector) Namespace(method string, payload []byte) string {
	r.calls++
	r.method = method
	r.payload = payload

	return r.ns
}

func (f allowlistFunc) Allows(service string) bool { return f(service) }

// ServiceNames has nothing to report: the interceptor only ever asks Allows.
func (f allowlistFunc) ServiceNames() []string { return nil }
