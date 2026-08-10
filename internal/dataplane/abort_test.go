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

// TestAbortFiresOnceWhenServingStopsUnexpectedly closes the listeners out from
// under the serving goroutines, which is the only way into the unexpected-exit
// path: Stop and cancelling the serving context are both clean shutdowns, and a
// taken port fails before serving starts.
func TestAbortFiresOnceWhenServingStopsUnexpectedly(t *testing.T) {
	t.Parallel()

	aborts := make(chan error, 2)

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
	require.Len(t, listeners, 2, "one upstream socket and the gateway")
	require.Equal(t, "unix", listeners[0].Addr().Network(), "upstream sockets bind before the gateway")
	require.Equal(t, "tcp", listeners[1].Addr().Network())

	for _, lis := range listeners {
		require.NoError(t, lis.Close())
	}

	select {
	case err := <-aborts:
		require.Error(t, err)
		require.ErrorContains(t, err, "stopped serving")
	case <-time.After(10 * time.Second):
		t.Fatal("Abort was not called after serving stopped")
	}

	// Both tiers stopped serving, but a caller is told once: the first report is
	// the one that brings the process down.
	select {
	case err := <-aborts:
		t.Fatalf("Abort fired more than once: %v", err)
	case <-time.After(500 * time.Millisecond):
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

// abortConfig points the only upstream at a template, so the plane binds a
// socket and serves without anything having to be reachable during Start. The
// socket path is derived from the hostPort, so naming the test in it keeps
// parallel planes off each other's sockets.
func abortConfig(t *testing.T) *config.Config {
	t.Helper()

	cfg := testConfig()
	cfg.Upstreams[0].Listen.HostPort = "{{ .LocalNamespace }}." + strings.ToLower(t.Name()) + ".example:7233"

	return cfg
}
