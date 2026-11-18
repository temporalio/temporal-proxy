package dataplane_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/fx"

	"go.temporal.io/proxy/internal/dataplane"
)

func TestModuleStartsAndStops(t *testing.T) {
	t.Parallel()

	var dp *dataplane.Dataplane

	app := fx.New(
		testGraph(newTestDeps(t, liveConfig(t))),
		dataplane.Module,
		fx.Populate(&dp),
		fx.NopLogger,
	)
	require.NoError(t, app.Err())

	startCtx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	require.NoError(t, app.Start(startCtx))

	require.NotNil(t, dp.Addr())

	stopCtx, stop := context.WithTimeout(context.WithoutCancel(t.Context()), 10*time.Second)
	defer stop()
	require.NoError(t, app.Stop(stopCtx))
}

// TestModuleShutsTheAppDownWhenServingStops proves the Abort the module
// provides reaches the Shutdowner: a plane that stops serving takes the application
// down non-zero, rather than leaving it up and answering nothing.
func TestModuleShutsTheAppDownWhenServingStops(t *testing.T) {
	t.Parallel()

	var dp *dataplane.Dataplane

	app := fx.New(
		testGraph(newTestDeps(t, abortConfig(t))),
		dataplane.Module,
		fx.Populate(&dp),
		fx.NopLogger,
	)
	require.NoError(t, app.Err())

	startCtx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	require.NoError(t, app.Start(startCtx))

	t.Cleanup(func() {
		stopCtx, stop := context.WithTimeout(context.WithoutCancel(t.Context()), 10*time.Second)
		defer stop()

		_ = app.Stop(stopCtx)
	})

	// Register for the signal before provoking it, so the wait cannot miss one
	// that arrives first.
	shutdown := app.Wait()

	lis := dp.Listener()
	require.NotNil(t, lis, "the gateway binds")
	require.NoError(t, lis.Close())

	select {
	case sig := <-shutdown:
		require.Equal(t, 1, sig.ExitCode, "an unexpected stop must exit non-zero")
	case <-time.After(10 * time.Second):
		t.Fatal("the application kept running after the dataplane stopped serving")
	}
}
