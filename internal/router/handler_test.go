package router_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/proto"

	"github.com/temporalio/temporal-proxy/internal/router"
	"github.com/temporalio/temporal-proxy/internal/server"
)

type (
	// recordingDirector captures what Resolve was called with and returns a fixed
	// connection.
	recordingDirector struct {
		cc *grpc.ClientConn

		mu        sync.Mutex
		calls     int
		method    string
		namespace string
		md        map[string][]string
	}

	// recordingReflector captures what Namespace was called with and returns a
	// fixed namespace. It is safe for the handler goroutine to write while the test
	// goroutine reads via snapshot.
	recordingReflector struct {
		ns string

		mu      sync.Mutex
		calls   int
		method  string
		payload []byte
	}

	stubDirector struct{ cc *grpc.ClientConn }

	stubReflector struct{}
)

func TestHandlerForwardsUnary(t *testing.T) {
	t.Parallel()

	relay := newRelayToUpstream(t, func(s *grpc.Server) {
		grpc_health_v1.RegisterHealthServer(s, health.NewServer())
	})

	resp, err := grpc_health_v1.NewHealthClient(relay).Check(t.Context(), &grpc_health_v1.HealthCheckRequest{})
	require.NoError(t, err)
	require.Equal(t, grpc_health_v1.HealthCheckResponse_SERVING, resp.GetStatus())
}

func TestHandlerForwardsServerStream(t *testing.T) {
	t.Parallel()

	relay := newRelayToUpstream(t, func(s *grpc.Server) {
		grpc_health_v1.RegisterHealthServer(s, health.NewServer())
	})

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	stream, err := grpc_health_v1.NewHealthClient(relay).Watch(ctx, &grpc_health_v1.HealthCheckRequest{})
	require.NoError(t, err)

	resp, err := stream.Recv()
	require.NoError(t, err)
	require.Equal(t, grpc_health_v1.HealthCheckResponse_SERVING, resp.GetStatus())
}

func TestHandlerPropagatesError(t *testing.T) {
	t.Parallel()

	relay := newRelayToUpstream(t, func(s *grpc.Server) {
		grpc_health_v1.RegisterHealthServer(s, health.NewServer())
	})

	_, err := grpc_health_v1.NewHealthClient(relay).Check(
		t.Context(),
		&grpc_health_v1.HealthCheckRequest{Service: "does-not-exist"},
	)
	require.Error(t, err)
	require.Equal(t, codes.NotFound, status.Code(err))
}

func TestHandlerRoutesUsingReflectorAndDirector(t *testing.T) {
	t.Parallel()

	echoDesc := grpc.ServiceDesc{
		ServiceName: "test.v1.Echo",
		HandlerType: (*any)(nil),
		Methods: []grpc.MethodDesc{
			{
				MethodName: "Ping",
				Handler: func(_ any, _ context.Context, dec func(any) error, _ grpc.UnaryServerInterceptor) (any, error) {
					in := new(grpc_health_v1.HealthCheckRequest)
					if err := dec(in); err != nil {
						return nil, err
					}
					return &grpc_health_v1.HealthCheckResponse{Status: grpc_health_v1.HealthCheckResponse_SERVING}, nil
				},
			},
		},
	}

	reflector := &recordingReflector{ns: "ns-from-reflector"}
	var director *recordingDirector
	relay := newRelayWith(
		t,
		func(s *grpc.Server) { s.RegisterService(&echoDesc, nil) },
		func(cc *grpc.ClientConn) router.Director {
			director = &recordingDirector{cc: cc}
			return director
		},
		reflector,
	)

	ctx := metadata.AppendToOutgoingContext(t.Context(), "x-route", "gold")
	resp := new(grpc_health_v1.HealthCheckResponse)
	require.NoError(t, relay.Invoke(ctx, "/test.v1.Echo/Ping", &grpc_health_v1.HealthCheckRequest{Service: "abc"}, resp))
	require.Equal(t, grpc_health_v1.HealthCheckResponse_SERVING, resp.GetStatus())

	// The Reflector is handed the method and the raw bytes of the first frame.
	rCalls, rMethod, rPayload := reflector.snapshot()
	require.Equal(t, 1, rCalls)
	require.Equal(t, "/test.v1.Echo/Ping", rMethod)
	gotReq := new(grpc_health_v1.HealthCheckRequest)
	require.NoError(t, proto.Unmarshal(rPayload, gotReq))
	require.Equal(t, "abc", gotReq.GetService())

	// The Director is handed the extracted namespace and the incoming metadata.
	dCalls, dMethod, dNS, dMD := director.snapshot()
	require.Equal(t, 1, dCalls)
	require.Equal(t, "/test.v1.Echo/Ping", dMethod)
	require.Equal(t, "ns-from-reflector", dNS)
	require.Equal(t, []string{"gold"}, dMD["x-route"])
}

func TestHandlerForwardsEmptyMessageHalfClose(t *testing.T) {
	t.Parallel()

	// Sum reads request messages until the client half-closes, then reports how
	// many it saw. It lets the test prove the upstream observed the half-close
	// (it returns rather than blocking) and received no injected first frame.
	countDesc := grpc.ServiceDesc{
		ServiceName: "test.v1.Count",
		HandlerType: (*any)(nil),
		Streams: []grpc.StreamDesc{
			{
				StreamName:    "Sum",
				ClientStreams: true,
				Handler: func(_ any, stream grpc.ServerStream) error {
					n := 0
					for {
						err := stream.RecvMsg(new(grpc_health_v1.HealthCheckRequest))
						if err == io.EOF {
							break
						}
						if err != nil {
							return err
						}
						n++
					}
					return stream.SendMsg(&grpc_health_v1.HealthCheckResponse{
						Status: grpc_health_v1.HealthCheckResponse_ServingStatus(n),
					})
				},
			},
		},
	}

	reflector := &recordingReflector{ns: "unused"}
	var director *recordingDirector
	relay := newRelayWith(
		t,
		func(s *grpc.Server) { s.RegisterService(&countDesc, nil) },
		func(cc *grpc.ClientConn) router.Director {
			director = &recordingDirector{cc: cc}
			return director
		},
		reflector,
	)

	stream, err := relay.NewStream(
		t.Context(),
		&grpc.StreamDesc{ClientStreams: true, ServerStreams: true},
		"/test.v1.Count/Sum",
	)
	require.NoError(t, err)
	require.NoError(t, stream.CloseSend()) // Half-close without ever sending a message.

	resp := new(grpc_health_v1.HealthCheckResponse)
	require.NoError(t, stream.RecvMsg(resp))
	require.Equal(t, 0, int(resp.GetStatus()), "upstream should observe zero request messages")
	require.ErrorIs(t, stream.RecvMsg(new(grpc_health_v1.HealthCheckResponse)), io.EOF)

	// With no first frame, there is nothing to peek, so the Reflector is skipped
	// and the Director routes with an empty namespace.
	rCalls, _, _ := reflector.snapshot()
	require.Zero(t, rCalls)

	dCalls, _, dNS, _ := director.snapshot()
	require.Equal(t, 1, dCalls)
	require.Empty(t, dNS)
}

func TestHandlerPropagatesHeaderAndTrailer(t *testing.T) {
	t.Parallel()

	echoDesc := grpc.ServiceDesc{
		ServiceName: "test.v1.Echo",
		HandlerType: (*any)(nil),
		Methods: []grpc.MethodDesc{
			{
				MethodName: "Ping",
				Handler: func(_ any, ctx context.Context, dec func(any) error, _ grpc.UnaryServerInterceptor) (any, error) {
					_ = grpc.SetHeader(ctx, metadata.Pairs("x-test-header", "hdr"))
					_ = grpc.SetTrailer(ctx, metadata.Pairs("x-test-trailer", "trl"))
					in := new(grpc_health_v1.HealthCheckRequest)
					if err := dec(in); err != nil {
						return nil, err
					}
					return &grpc_health_v1.HealthCheckResponse{Status: grpc_health_v1.HealthCheckResponse_SERVING}, nil
				},
			},
		},
	}

	relay := newRelayToUpstream(t, func(s *grpc.Server) {
		s.RegisterService(&echoDesc, nil)
	})

	var header, trailer metadata.MD
	resp := new(grpc_health_v1.HealthCheckResponse)
	err := relay.Invoke(
		t.Context(),
		"/test.v1.Echo/Ping",
		&grpc_health_v1.HealthCheckRequest{},
		resp,
		grpc.Header(&header),
		grpc.Trailer(&trailer),
	)
	require.NoError(t, err)
	require.Equal(t, []string{"hdr"}, header.Get("x-test-header"))
	require.Equal(t, []string{"trl"}, trailer.Get("x-test-trailer"))
}

func TestHandlerForwardsHeaderOnlyResponse(t *testing.T) {
	t.Parallel()

	streamDesc := grpc.ServiceDesc{
		ServiceName: "test.v1.Stream",
		HandlerType: (*any)(nil),
		Streams: []grpc.StreamDesc{
			{
				StreamName:    "HeaderOnly",
				ServerStreams: true,
				Handler: func(_ any, stream grpc.ServerStream) error {
					return stream.SetHeader(metadata.Pairs("x-header-only", "yes"))
				},
			},
		},
	}

	relay := newRelayToUpstream(t, func(s *grpc.Server) {
		s.RegisterService(&streamDesc, nil)
	})

	stream, err := relay.NewStream(
		t.Context(),
		&grpc.StreamDesc{ServerStreams: true},
		"/test.v1.Stream/HeaderOnly",
	)
	require.NoError(t, err)
	require.NoError(t, stream.SendMsg(&grpc_health_v1.HealthCheckRequest{}))
	require.NoError(t, stream.CloseSend())

	md, err := stream.Header()
	require.NoError(t, err)
	require.Equal(t, []string{"yes"}, md.Get("x-header-only"))

	// Stream completes cleanly with no message.
	require.ErrorIs(t, stream.RecvMsg(new(grpc_health_v1.HealthCheckResponse)), io.EOF)
}

func TestHandlerCoHostsLocalHealthWithForwarding(t *testing.T) {
	t.Parallel()

	echoDesc := grpc.ServiceDesc{
		ServiceName: "test.v1.Echo",
		HandlerType: (*any)(nil),
		Methods: []grpc.MethodDesc{
			{
				MethodName: "Ping",
				Handler: func(_ any, _ context.Context, dec func(any) error, _ grpc.UnaryServerInterceptor) (any, error) {
					in := new(grpc_health_v1.HealthCheckRequest)
					if err := dec(in); err != nil {
						return nil, err
					}
					return &grpc_health_v1.HealthCheckResponse{Status: grpc_health_v1.HealthCheckResponse_SERVING}, nil
				},
			},
		},
	}

	upstreamLis := bufconn.Listen(1024 * 1024)
	upstream := grpc.NewServer()
	upstream.RegisterService(&echoDesc, nil)
	serve(t, upstream, upstreamLis)

	upstreamConn := dialBufconn(t, upstreamLis)
	t.Cleanup(func() { _ = upstreamConn.Close() })

	svr, err := server.New(
		server.WithUnknownServiceHandler(router.Handler(stubDirector{upstreamConn}, stubReflector{})),
		server.WithServerCodec(router.Codec()),
	)
	require.NoError(t, err)

	relayLis := bufconn.Listen(1024 * 1024)
	errCh := make(chan error, 1)
	go func() { errCh <- svr.Start(t.Context(), relayLis) }()

	relayConn := dialBufconn(t, relayLis)
	t.Cleanup(func() { _ = relayConn.Close() })

	hResp, err := grpc_health_v1.NewHealthClient(relayConn).Check(t.Context(), &grpc_health_v1.HealthCheckRequest{})
	require.NoError(t, err)
	require.Equal(t, grpc_health_v1.HealthCheckResponse_SERVING, hResp.GetStatus())

	eResp := new(grpc_health_v1.HealthCheckResponse)
	require.NoError(t, relayConn.Invoke(t.Context(), "/test.v1.Echo/Ping", &grpc_health_v1.HealthCheckRequest{}, eResp))
	require.Equal(t, grpc_health_v1.HealthCheckResponse_SERVING, eResp.GetStatus())

	require.NoError(t, svr.Stop(t.Context()))
	<-errCh
}

func TestStatusError(t *testing.T) {
	t.Parallel()

	statusErr := status.Error(codes.NotFound, "nope")

	tests := []struct {
		name string
		err  error
		want codes.Code
	}{
		{name: "preserves existing gRPC status", err: statusErr, want: codes.NotFound},
		{name: "maps context cancellation", err: context.Canceled, want: codes.Canceled},
		{name: "maps context deadline", err: context.DeadlineExceeded, want: codes.DeadlineExceeded},
		{name: "wraps unknown error as internal", err: errors.New("boom"), want: codes.Internal},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tt.want, status.Code(router.StatusError(tt.err)))
		})
	}

	t.Run("returns the existing status error verbatim", func(t *testing.T) {
		t.Parallel()
		require.ErrorIs(t, router.StatusError(statusErr), statusErr)
	})
}

func (s stubDirector) Resolve(context.Context, string, string, map[string][]string) (*grpc.ClientConn, error) {
	return s.cc, nil
}

func (stubReflector) Namespace(string, []byte) string { return "" }

func (r *recordingReflector) Namespace(method string, payload []byte) string {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls++
	r.method = method
	r.payload = bytes.Clone(payload)
	return r.ns
}

func (r *recordingReflector) snapshot() (calls int, method string, payload []byte) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.calls, r.method, bytes.Clone(r.payload)
}

func (d *recordingDirector) Resolve(_ context.Context, method, namespace string, md map[string][]string) (*grpc.ClientConn, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.calls++
	d.method = method
	d.namespace = namespace
	d.md = md
	return d.cc, nil
}

func (d *recordingDirector) snapshot() (calls int, method, namespace string, md map[string][]string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.calls, d.method, d.namespace, d.md
}

func dialBufconn(t *testing.T, lis *bufconn.Listener) *grpc.ClientConn {
	t.Helper()
	conn, err := grpc.NewClient(
		"passthrough:///bufnet",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return lis.Dial()
		}),
	)
	require.NoError(t, err)
	conn.Connect()
	return conn
}

func serve(t *testing.T, srv *grpc.Server, lis *bufconn.Listener) {
	t.Helper()
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)
}

// newRelayToUpstream stands up a fake upstream (configured by registerUpstream),
// then a bare relay server that forwards all methods to it via router.Handler
// using pass-through routing. Returns a client conn pointed at the relay.
func newRelayToUpstream(t *testing.T, registerUpstream func(*grpc.Server)) *grpc.ClientConn {
	t.Helper()

	return newRelayWith(
		t, registerUpstream,
		func(cc *grpc.ClientConn) router.Director { return stubDirector{cc} },
		stubReflector{},
	)
}

// newRelayWith is like newRelayToUpstream but lets the caller supply the Director
// and Reflector, so tests can observe how the handler routes. makeDirector
// receives the connection to the fake upstream.
func newRelayWith(
	t *testing.T,
	registerUpstream func(*grpc.Server),
	makeDirector func(cc *grpc.ClientConn) router.Director,
	reflector router.Reflector,
) *grpc.ClientConn {
	t.Helper()

	upstreamLis := bufconn.Listen(1024 * 1024)
	upstream := grpc.NewServer()
	registerUpstream(upstream)
	serve(t, upstream, upstreamLis)

	upstreamConn := dialBufconn(t, upstreamLis)
	t.Cleanup(func() { _ = upstreamConn.Close() })

	relayLis := bufconn.Listen(1024 * 1024)
	relay := grpc.NewServer(
		grpc.ForceServerCodecV2(router.Codec()),
		grpc.UnknownServiceHandler(router.Handler(makeDirector(upstreamConn), reflector)),
	)
	serve(t, relay, relayLis)

	relayConn := dialBufconn(t, relayLis)
	t.Cleanup(func() { _ = relayConn.Close() })
	return relayConn
}
