package connect_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/fx"
	"google.golang.org/grpc/connectivity"

	"go.temporal.io/proxy/internal/transport/connect"
)

func TestModule(t *testing.T) {
	t.Parallel()

	t.Run("provides a usable Pool", func(t *testing.T) {
		t.Parallel()

		var p *connect.Pool
		app := fx.New(connect.Module, fx.Populate(&p), fx.NopLogger)
		require.NoError(t, app.Err())
		require.NotNil(t, p)

		require.NoError(t, p.Set("host", newConn(t)))
	})

	t.Run("closes the pool on stop", func(t *testing.T) {
		t.Parallel()

		var p *connect.Pool
		app := fx.New(connect.Module, fx.Populate(&p), fx.NopLogger)
		require.NoError(t, app.Err())

		conn := newConn(t)
		require.NoError(t, p.Set("host", conn))

		startCtx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
		defer cancel()
		require.NoError(t, app.Start(startCtx))

		stopCtx, stopCancel := context.WithTimeout(t.Context(), 5*time.Second)
		defer stopCancel()
		require.NoError(t, app.Stop(stopCtx))

		// The stop hook calls Pool.Close, which closes every connection. A
		// closed gRPC client connection transitions to Shutdown, so this
		// proves the hook actually ran rather than just returning nil.
		require.Equal(t, connectivity.Shutdown, conn.GetState())
	})
	t.Run("closes the pool after the components that use it stop", func(t *testing.T) {
		t.Parallel()

		conn := newConn(t)

		var stateAtStop connectivity.State

		// The user's hook is registered from an Invoke listed before the module,
		// so a pool hook registered by the module's own Invoke would come later
		// and run first on stop.
		app := fx.New(
			fx.Invoke(func(lc fx.Lifecycle, p *connect.Pool) error {
				lc.Append(fx.StopHook(func() { stateAtStop = conn.GetState() }))

				return p.Set("host", conn)
			}),
			connect.Module,
			fx.NopLogger,
		)
		require.NoError(t, app.Err())

		startCtx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
		defer cancel()
		require.NoError(t, app.Start(startCtx))

		stopCtx, stopCancel := context.WithTimeout(t.Context(), 5*time.Second)
		defer stopCancel()
		require.NoError(t, app.Stop(stopCtx))

		require.NotEqual(t, connectivity.Shutdown, stateAtStop, "the pool closed before its user stopped")
		require.Equal(t, connectivity.Shutdown, conn.GetState())
	})
}
