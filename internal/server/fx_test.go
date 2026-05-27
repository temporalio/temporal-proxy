package server_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/log"
	"go.uber.org/fx"
	"google.golang.org/grpc/health/grpc_health_v1"

	"github.com/temporalio/temporal-proxy/internal/config"
	"github.com/temporalio/temporal-proxy/internal/server"
)

func TestModule(t *testing.T) {
	t.Parallel()

	t.Run("wires defaults and runs the lifecycle", func(t *testing.T) {
		t.Parallel()

		app := newTestApp(t,
			fx.Supply(&config.Config{
				Listen: config.ListenConfig{HostPort: "127.0.0.1:0"},
			}),
		)

		startCtx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
		defer cancel()
		require.NoError(t, app.Start(startCtx))

		stopCtx, stopCancel := context.WithTimeout(t.Context(), 5*time.Second)
		defer stopCancel()
		require.NoError(t, app.Stop(stopCtx))
	})

	t.Run("honours every optional dependency", func(t *testing.T) {
		t.Parallel()

		hc := server.HealthCheckFunc(time.Hour, func(context.Context) grpc_health_v1.HealthCheckResponse_ServingStatus {
			return grpc_health_v1.HealthCheckResponse_SERVING
		})

		app := newTestApp(t,
			fx.Supply(
				&config.Config{Listen: config.ListenConfig{HostPort: "127.0.0.1:0"}},
				fx.Annotate(hc, fx.As(new(server.HealthCheck))),
			),
			fx.Provide(func() log.Logger { return log.NewNoopLogger() }),
		)

		startCtx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
		defer cancel()
		require.NoError(t, app.Start(startCtx))

		stopCtx, stopCancel := context.WithTimeout(t.Context(), 5*time.Second)
		defer stopCancel()
		require.NoError(t, app.Stop(stopCtx))
	})

	t.Run("surfaces server construction errors", func(t *testing.T) {
		t.Parallel()

		missing := filepath.Join(t.TempDir(), "missing.pem")

		app := fx.New(
			fx.Supply(
				fx.Annotate(t.Context(), fx.As(new(context.Context))),
				&config.Config{
					Listen: config.ListenConfig{
						HostPort: "127.0.0.1:0",
						TLS: &config.TLSConfig{
							CertFile: missing,
							KeyFile:  missing,
						},
					},
				},
			),
			server.Module,
			fx.NopLogger,
		)

		require.Error(t, app.Err())
		require.ErrorIs(t, app.Err(), os.ErrNotExist)
	})

	t.Run("surfaces listener creation errors at start", func(t *testing.T) {
		t.Parallel()

		app := newTestApp(t,
			fx.Supply(&config.Config{
				Listen: config.ListenConfig{HostPort: "not-a-valid-host:port"},
			}),
		)

		startCtx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
		defer cancel()

		err := app.Start(startCtx)
		require.Error(t, err)
		require.ErrorContains(t, err, "failed to create listener")
	})
}

func newTestApp(t *testing.T, opts ...fx.Option) *fx.App {
	t.Helper()

	base := []fx.Option{
		fx.Supply(fx.Annotate(t.Context(), fx.As(new(context.Context)))),
		server.Module,
		fx.NopLogger,
	}

	return fx.New(append(base, opts...)...)
}
