package dataplane_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/fx"

	"go.temporal.io/proxy/internal/dataplane"
)

func TestModuleStartsAndStops(t *testing.T) {
	t.Parallel()

	p, err := newPlane(t, newTestDeps(t, liveConfig(t)))
	require.NoError(t, err)

	startCtx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	require.NoError(t, p.Start(startCtx))

	require.NotNil(t, p.Addr())

	stopCtx, stop := context.WithTimeout(context.WithoutCancel(t.Context()), 10*time.Second)
	defer stop()
	require.NoError(t, p.Stop(stopCtx))
}

// TestModuleAbortShutsTheAppDown proves the Abort the module provides reaches
// the Shutdowner: a gateway that stops serving takes the application down
// non-zero, rather than leaving it up and answering nothing. The gateway's side,
// calling Abort on an unexpected stop, is covered in internal/server.
func TestModuleAbortShutsTheAppDown(t *testing.T) {
	t.Parallel()

	var abort dataplane.Abort

	p, err := newPlane(t, newTestDeps(t, abortConfig(t)), fx.Populate(&abort))
	require.NoError(t, err)

	startCtx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	require.NoError(t, p.Start(startCtx))

	t.Cleanup(func() {
		stopCtx, stop := context.WithTimeout(context.WithoutCancel(t.Context()), 10*time.Second)
		defer stop()

		_ = p.Stop(stopCtx)
	})

	// Register for the signal before provoking it, so the wait cannot miss one
	// that arrives first.
	shutdown := p.app.Wait()

	abort(errors.New("gateway stopped serving"))

	select {
	case sig := <-shutdown:
		require.Equal(t, 1, sig.ExitCode, "an unexpected stop must exit non-zero")
	case <-time.After(10 * time.Second):
		t.Fatal("the application kept running after Abort")
	}
}
