package router_test

import (
	"context"
	"errors"
	"io"
	"net"
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

	"github.com/temporalio/temporal-proxy/internal/router"
	"github.com/temporalio/temporal-proxy/internal/server"
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
		server.WithUnknownServiceHandler(router.Handler(upstreamConn)),
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
// then a bare relay server that forwards all methods to it via router.Handler.
// Returns a client conn pointed at the relay.
func newRelayToUpstream(t *testing.T, registerUpstream func(*grpc.Server)) *grpc.ClientConn {
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
		grpc.UnknownServiceHandler(router.Handler(upstreamConn)),
	)
	serve(t, relay, relayLis)

	relayConn := dialBufconn(t, relayLis)
	t.Cleanup(func() { _ = relayConn.Close() })
	return relayConn
}
