package proxy_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/fx"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health/grpc_health_v1"

	"github.com/temporalio/temporal-proxy/internal/config"
	"github.com/temporalio/temporal-proxy/internal/proxy"
	"github.com/temporalio/temporal-proxy/pkg/logger"
	"github.com/temporalio/temporal-proxy/pkg/validation"
)

func TestModule(t *testing.T) {
	t.Parallel()

	t.Run("wires defaults and runs the lifecycle", func(t *testing.T) {
		t.Parallel()

		const upstream = "127.0.0.1:47233"

		app := newProxyApp(t, &config.Config{
			Upstreams: []config.Upstream{{Name: "primary", Listen: config.ListenConfig{HostPort: upstream}}},
		})
		require.NoError(t, app.Err())

		startServeStop(t, app, upstream)
	})

	t.Run("uses the supplied logger", func(t *testing.T) {
		t.Parallel()

		const upstream = "127.0.0.1:57233"

		log := logger.NewTestLogger()
		app := newProxyApp(
			t,
			&config.Config{Upstreams: []config.Upstream{{Name: "primary", Listen: config.ListenConfig{HostPort: upstream}}}},
			fx.Provide(func() logger.Logger { return log }),
		)
		require.NoError(t, app.Err())

		startServeStop(t, app, upstream)

		require.True(t, log.Contains("Starting the server"), "expected the injected logger to be used")
	})

	t.Run("rejects invalid upstream configuration before construction", func(t *testing.T) {
		t.Parallel()

		app := newProxyApp(t, &config.Config{
			Upstreams: []config.Upstream{{Name: "primary", Listen: config.ListenConfig{HostPort: "not-a-host-port"}}},
		})

		require.Error(t, app.Err())
		require.ErrorContains(t, app.Err(), "invalid upstream configuration")

		var errs validation.Errors
		require.ErrorAs(t, app.Err(), &errs, "expected validation.Errors in chain")
		require.NotEmpty(t, errs)
	})
}

func newProxyApp(t *testing.T, cfg *config.Config, opts ...fx.Option) *fx.App {
	t.Helper()

	base := []fx.Option{
		fx.Supply(fx.Annotate(t.Context(), fx.As(new(context.Context)))),
		fx.Supply(cfg),
		proxy.Module,
		fx.NopLogger,
	}

	return fx.New(append(base, opts...)...)
}

// startServeStop starts the app, confirms the proxy serves on its unix socket
// via the local health service, then stops the app.
func startServeStop(t *testing.T, app *fx.App, upstream string) {
	t.Helper()

	startCtx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	require.NoError(t, app.Start(startCtx))

	conn := dialUnix(t, upstream)
	defer func() { _ = conn.Close() }()

	resp, err := grpc_health_v1.NewHealthClient(conn).Check(
		startCtx,
		&grpc_health_v1.HealthCheckRequest{},
		grpc.WaitForReady(true),
	)
	require.NoError(t, err)
	require.Equal(t, grpc_health_v1.HealthCheckResponse_SERVING, resp.GetStatus())

	stopCtx, stopCancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer stopCancel()
	require.NoError(t, app.Stop(stopCtx))
}
