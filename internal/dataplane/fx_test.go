package dataplane_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/fx"

	"github.com/temporalio/temporal-proxy/internal/auth"
	"github.com/temporalio/temporal-proxy/internal/config"
	"github.com/temporalio/temporal-proxy/internal/dataplane"
	"github.com/temporalio/temporal-proxy/internal/services"
	"github.com/temporalio/temporal-proxy/pkg/logger"
)

func TestModuleStartsAndStops(t *testing.T) {
	t.Parallel()

	var dp *dataplane.Dataplane

	app := fx.New(
		testGraph(t, liveConfig(t)),
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

// TestModuleShutsTheAppDownWhenServingStops proves the Abort the module hands
// New reaches the Shutdowner: a plane that stops serving takes the application
// down non-zero, rather than leaving it up and answering nothing.
func TestModuleShutsTheAppDownWhenServingStops(t *testing.T) {
	t.Parallel()

	var dp *dataplane.Dataplane

	app := fx.New(
		testGraph(t, abortConfig(t)),
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

	for _, lis := range dp.Listeners() {
		require.NoError(t, lis.Close())
	}

	select {
	case sig := <-shutdown:
		require.Equal(t, 1, sig.ExitCode, "an unexpected stop must exit non-zero")
	case <-time.After(10 * time.Second):
		t.Fatal("the application kept running after the dataplane stopped serving")
	}
}

// testGraph supplies every dependency dataplane.Module needs, each read off the
// same testDeps the direct-construction path in this package uses, so the two
// cannot drift apart.
func testGraph(t *testing.T, cfg *config.Config) fx.Option {
	t.Helper()

	d := newTestDeps(t, cfg)

	return fx.Options(
		fx.Supply(fx.Annotate(d.ctx, fx.As(new(context.Context)))),
		fx.Supply(d.cfg),
		fx.Supply(d.extractor),
		fx.Supply(d.translator),
		fx.Supply(d.pool),
		fx.Supply(d.metrics),
		fx.Provide(func() services.Allowlist { return d.allowlist }),
		fx.Supply(fx.Annotate(d.auth, fx.As(new(auth.Authenticator)))),
		fx.Supply(fx.Annotate(d.logger, fx.As(new(logger.Logger)))),
		fx.Supply(d.vault),
	)
}
