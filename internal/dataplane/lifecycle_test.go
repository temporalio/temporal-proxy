package dataplane_test

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health/grpc_health_v1"

	"github.com/temporalio/temporal-proxy/internal/config"
	"github.com/temporalio/temporal-proxy/internal/dataplane"
	"github.com/temporalio/temporal-proxy/internal/dataplane/dataplanetest"
)

func TestStartReturnsOnceTheGatewayAccepts(t *testing.T) {
	t.Parallel()

	cfg := liveConfig(t)
	dp := startPlane(t, newTestDeps(t, cfg))

	require.NotNil(t, dp.Addr())

	gw, err := net.Dial("tcp", dp.Addr().String())
	require.NoError(t, err, "the gateway must be accepting once Start returns")
	require.NoError(t, gw.Close())
}

func TestStartResolvesPortZero(t *testing.T) {
	t.Parallel()

	cfg := liveConfig(t)
	cfg.Listen.HostPort = "127.0.0.1:0"

	dp := startPlane(t, newTestDeps(t, cfg))

	addr, ok := dp.Addr().(*net.TCPAddr)
	require.True(t, ok)
	require.NotZero(t, addr.Port, "Addr reports the port the kernel picked")
}

func TestStartFailsOnUnreachableStaticUpstream(t *testing.T) {
	t.Parallel()

	cfg := liveConfig(t)
	cfg.Upstreams[0].Listen.HostPort = dataplanetest.DeadUpstream(t)

	dp, err := dataplane.New(t.Context(), cfg, newTestDeps(t, cfg).opts()...)
	require.NoError(t, err)
	t.Cleanup(func() { _ = dp.Stop(context.WithoutCancel(t.Context())) })

	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()

	require.ErrorContains(
		t,
		dp.Start(ctx),
		"upstream connection not ready",
		"an unreachable static upstream must fail startup",
	)

	// A failed Start rolls back: the gateway never bound.
	require.Nil(t, dp.Addr())
}

func TestStartFailsWhenTheGatewayPortIsTaken(t *testing.T) {
	t.Parallel()

	cfg := liveConfig(t)
	first := startPlane(t, newTestDeps(t, cfg))

	// A second plane on the address the first is already accepting on cannot
	// bind, so its Start fails after its upstream connections are open.
	cfg = liveConfig(t)
	cfg.Listen.HostPort = first.Addr().String()

	second, err := dataplane.New(t.Context(), cfg, newTestDeps(t, cfg).opts()...)
	require.NoError(t, err)
	t.Cleanup(func() { _ = second.Stop(context.WithoutCancel(t.Context())) })

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	require.ErrorContains(t, second.Start(ctx), "failed to create listener")
	require.Nil(t, second.Addr())

	// Rolling the second plane back must not disturb the first.
	conn, err := net.Dial("tcp", first.Addr().String())
	require.NoError(t, err, "the first plane must still be serving")
	require.NoError(t, conn.Close())
}

func TestCleanShutdownDoesNotAbort(t *testing.T) {
	t.Parallel()

	aborts := make(chan error, 1)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	cfg := liveConfig(t)
	d := newTestDeps(t, cfg)

	dp, err := dataplane.New(ctx, cfg, append(d.opts(), dataplane.WithAbort(func(err error) { aborts <- err }))...)
	require.NoError(t, err)

	startCtx, startCancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer startCancel()
	require.NoError(t, dp.Start(startCtx))

	// Cancelling the serving context ends the health check, not serving; Stop is
	// what shuts the plane down. Neither is a failure, and either one reporting
	// an abort would take the whole process down on a normal shutdown.
	cancel()
	require.NoError(t, dp.Stop(context.WithoutCancel(t.Context())))

	select {
	case err := <-aborts:
		t.Fatalf("Abort fired during a clean shutdown: %v", err)
	case <-time.After(500 * time.Millisecond):
	}
}

func TestStopWithoutStart(t *testing.T) {
	t.Parallel()

	cfg := liveConfig(t)
	dp, err := dataplane.New(t.Context(), cfg, newTestDeps(t, cfg).opts()...)
	require.NoError(t, err)

	require.NoError(t, dp.Stop(t.Context()), "Stop before Start has nothing to drain")
}

func TestStartWithTemplatedAndStaticUpstreams(t *testing.T) {
	t.Parallel()

	cfg := liveConfig(t)
	cfg.Upstreams[0].Namespaces = config.NamespaceConfig{Rules: config.NamespaceRules{Suffix: ".remote"}}
	cfg.Upstreams = append(cfg.Upstreams, config.Upstream{
		Name:   "templated",
		Listen: config.ListenConfig{HostPort: "{{ .LocalNamespace }}.dataplane-lifecycle.example:7233"},
	})

	// Nothing is listening for the templated upstream, which is the point: it
	// resolves per request, so it is excluded from the readiness wait and Start
	// still succeeds.
	dp := startPlane(t, newTestDeps(t, cfg))
	require.NotNil(t, dp.Addr())
}

func TestStopClosesTheGateway(t *testing.T) {
	t.Parallel()

	cfg := liveConfig(t)
	dp, err := dataplane.New(t.Context(), cfg, newTestDeps(t, cfg).opts()...)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	require.NoError(t, dp.Start(ctx))

	addr := dp.Addr().String()
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })

	resp, err := grpc_health_v1.NewHealthClient(conn).Check(
		t.Context(), &grpc_health_v1.HealthCheckRequest{},
	)
	require.NoError(t, err, "the gateway must be serving before Stop")
	require.Equal(t, grpc_health_v1.HealthCheckResponse_SERVING, resp.GetStatus())

	require.NoError(t, dp.Stop(context.WithoutCancel(t.Context())))

	gw, err := net.Dial("tcp", addr)
	if err == nil {
		_ = gw.Close()
		t.Fatalf("the gateway is still accepting on %s", addr)
	}
}

// liveConfig is testConfig pointed at an upstream that is actually listening,
// which Start must reach before the gateway binds.
func liveConfig(t *testing.T) *config.Config {
	t.Helper()

	cfg := testConfig()
	cfg.Upstreams[0].Listen.HostPort = dataplanetest.NewUpstream(t).Addr()

	return cfg
}

func startPlane(t *testing.T, d testDeps) *dataplane.Dataplane {
	t.Helper()

	dp, err := dataplane.New(d.ctx, d.cfg, d.opts()...)
	require.NoError(t, err)

	// startCtx is cancelled before this returns, so every caller goes on to use a
	// plane whose startup context is dead. That is the point: it bounds startup
	// only, and serving continues on the context New was given.
	startCtx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	require.NoError(t, dp.Start(startCtx))

	// t.Context is already cancelled by the time cleanups run, and a shutdown
	// must not inherit that.
	t.Cleanup(func() { _ = dp.Stop(context.WithoutCancel(t.Context())) })

	return dp
}
