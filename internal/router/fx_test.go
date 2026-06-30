package router_test

import (
	"context"
	"net"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/fx"
	"google.golang.org/grpc"
	"google.golang.org/grpc/encoding"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/test/bufconn"

	"github.com/temporalio/temporal-proxy/internal/config"
	"github.com/temporalio/temporal-proxy/internal/router"
	"github.com/temporalio/temporal-proxy/internal/transport/socket"
)

func TestModule(t *testing.T) {
	t.Parallel()

	t.Run("provides a codec and a stream handler", func(t *testing.T) {
		t.Parallel()

		var (
			codec   encoding.CodecV2
			handler grpc.StreamHandler
		)

		app := fx.New(
			fx.Supply(&config.Config{
				Upstreams: []config.Upstream{{Name: "primary", Listen: config.ListenConfig{HostPort: "127.0.0.1:7233"}}},
			}),
			router.Module,
			fx.Populate(&codec, &handler),
			fx.NopLogger,
		)
		require.NoError(t, app.Err())

		startCtx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
		defer cancel()
		require.NoError(t, app.Start(startCtx))

		require.NotNil(t, codec)
		require.Equal(t, "proto", codec.Name())
		require.NotNil(t, handler)

		stopCtx, stopCancel := context.WithTimeout(t.Context(), 5*time.Second)
		defer stopCancel()
		require.NoError(t, app.Stop(stopCtx))
	})

	t.Run("provided handler forwards to the proxy socket", func(t *testing.T) {
		t.Parallel()

		// A unique upstream yields a unique socket path so the stand-in proxy
		// does not collide with other tests. It is never dialed directly here.
		const upstream = "127.0.0.3:7233"

		sockPath, err := socket.UnixPath(upstream)
		require.NoError(t, err)

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

		// Stand up a stand-in proxy on the exact socket the module derives.
		_ = os.Remove(sockPath)
		proxyLis, err := net.Listen("unix", sockPath)
		require.NoError(t, err)

		fakeProxy := grpc.NewServer()
		fakeProxy.RegisterService(&echoDesc, nil)
		go func() { _ = fakeProxy.Serve(proxyLis) }()
		t.Cleanup(fakeProxy.Stop)

		var (
			codec   encoding.CodecV2
			handler grpc.StreamHandler
		)
		app := fx.New(
			fx.Supply(&config.Config{
				Upstreams: []config.Upstream{{Name: "primary", Listen: config.ListenConfig{HostPort: upstream}}},
			}),
			router.Module,
			fx.Populate(&codec, &handler),
			fx.NopLogger,
		)
		startCtx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
		defer cancel()
		require.NoError(t, app.Start(startCtx))

		// Mount the provided codec + handler on a relay and confirm an unknown
		// method is forwarded over the socket to the stand-in proxy.
		relayLis := bufconn.Listen(1024 * 1024)
		relay := grpc.NewServer(
			grpc.ForceServerCodecV2(codec),
			grpc.UnknownServiceHandler(handler),
		)
		serve(t, relay, relayLis)

		relayConn := dialBufconn(t, relayLis)
		t.Cleanup(func() { _ = relayConn.Close() })

		resp := new(grpc_health_v1.HealthCheckResponse)
		require.NoError(t, relayConn.Invoke(
			t.Context(),
			"/test.v1.Echo/Ping",
			&grpc_health_v1.HealthCheckRequest{},
			resp,
		))
		require.Equal(t, grpc_health_v1.HealthCheckResponse_SERVING, resp.GetStatus())

		stopCtx, stopCancel := context.WithTimeout(t.Context(), 5*time.Second)
		defer stopCancel()
		require.NoError(t, app.Stop(stopCtx))
	})
}
