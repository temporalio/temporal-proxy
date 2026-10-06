package dataplane_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/temporalio/temporal-proxy/internal/config"
	"github.com/temporalio/temporal-proxy/internal/dataplane"
	"github.com/temporalio/temporal-proxy/pkg/logger"
)

// TestAbortFiresWhenServingStopsUnexpectedly closes the gateway's listener out
// from under its serving goroutine, which is the only way into the unexpected-exit
// path: Stop and cancelling the serving context are both clean shutdowns, and a
// taken port fails before serving starts.
func TestAbortFiresWhenServingStopsUnexpectedly(t *testing.T) {
	t.Parallel()

	aborts := make(chan error, 1)

	cfg := abortConfig(t)
	d := newTestDeps(t, cfg)

	dp, err := dataplane.New(
		t.Context(), cfg, append(d.opts(), dataplane.WithAbort(func(err error) { aborts <- err }))...,
	)
	require.NoError(t, err)

	startCtx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	require.NoError(t, dp.Start(startCtx))

	t.Cleanup(func() { _ = dp.Stop(context.WithoutCancel(t.Context())) })

	listeners := dp.Listeners()
	require.Len(t, listeners, 1, "only the gateway binds")
	require.NoError(t, listeners[0].Close())

	select {
	case err := <-aborts:
		require.Error(t, err)
		require.ErrorContains(t, err, "stopped serving")
	case <-time.After(10 * time.Second):
		t.Fatal("Abort was not called after serving stopped")
	}
}

func TestAbortIsOptional(t *testing.T) {
	t.Parallel()

	log := logger.NewTestLogger()

	cfg := abortConfig(t)
	d := newTestDeps(t, cfg)
	d.logger = log

	// A nil Abort drops the notification rather than panicking; the log entry is
	// then the only record.
	dp := startPlane(t, d)

	for _, lis := range dp.Listeners() {
		require.NoError(t, lis.Close())
	}

	require.Eventually(
		t,
		func() bool { return log.Contains("Dataplane stopped serving") },
		10*time.Second,
		10*time.Millisecond,
	)
}

// abortConfig points the only upstream at a template, so the plane serves
// without anything having to be reachable during Start.
func abortConfig(t *testing.T) *config.Config {
	t.Helper()

	cfg := testConfig()
	cfg.Upstreams[0].Listen.HostPort = "{{ .LocalNamespace }}." + strings.ToLower(t.Name()) + ".example:7233"

	return cfg
}
