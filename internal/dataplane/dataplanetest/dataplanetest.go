package dataplanetest

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/stretchr/testify/require"
	"go.temporal.io/api/workflowservice/v1"
	"go.uber.org/fx"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/reflect/protoregistry"

	"github.com/temporalio/temporal-proxy/internal/api"
	"github.com/temporalio/temporal-proxy/internal/auth"
	"github.com/temporalio/temporal-proxy/internal/config"
	"github.com/temporalio/temporal-proxy/internal/dataplane"
	"github.com/temporalio/temporal-proxy/internal/kms"
	"github.com/temporalio/temporal-proxy/internal/metrics"
	"github.com/temporalio/temporal-proxy/internal/protoutil"
	"github.com/temporalio/temporal-proxy/internal/services"
	"github.com/temporalio/temporal-proxy/internal/transport/connect"
	"github.com/temporalio/temporal-proxy/pkg/logger"
)

const (
	// DefaultUpstream is the name [Config] gives the upstream it wires up.
	DefaultUpstream = "workers"

	// lifecycleTimeout bounds startup and shutdown. Both only wait on loopback
	// sockets, so anything near this is a hang rather than slowness.
	lifecycleTimeout = 10 * time.Second

	// requestTimeout bounds one call made through the fixture. Callers pass
	// grpc.WaitForReady, which blocks rather than failing while a connection is
	// down, so without a deadline a plane that never becomes routable hangs the
	// test until the package timeout instead of failing it.
	requestTimeout = 10 * time.Second
)

// Fixture is a running dataplane and the connections needed to drive it. It
// stops when the test ends.
type Fixture struct {
	t    *testing.T
	dp   *dataplane.Dataplane
	conn *grpc.ClientConn
}

// Config returns a minimal valid configuration: an ephemeral gateway port and
// one upstream named [DefaultUpstream] pointed at up, routed to by default.
func Config(up *Upstream) *config.Config {
	return &config.Config{
		Listen:  config.ListenConfig{HostPort: "127.0.0.1:0"},
		Routing: config.Routing{DefaultUpstream: DefaultUpstream},
		Upstreams: config.UpstreamList{{
			Name:   DefaultUpstream,
			Listen: config.ListenConfig{HostPort: up.Addr(), TLS: up.TLSConfig()},
		}},
	}
}

// DeadUpstream returns a loopback address with nothing behind it, by taking a
// port from the kernel and immediately giving it back. The window in which
// something else could claim that port is small enough to live with in a test,
// and it is the only way to name an address that is guaranteed unreachable now
// and free later.
func DeadUpstream(t *testing.T) string {
	t.Helper()

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	addr := lis.Addr().String()
	require.NoError(t, lis.Close())

	return addr
}

// DialUnix returns a client connection to the unix socket at path, closed when
// the test ends.
func DialUnix(t *testing.T, path string) *grpc.ClientConn {
	t.Helper()

	conn, err := grpc.NewClient("unix://"+path, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })

	return conn
}

// Start constructs a dataplane directly, the way the production fx module
// does, and starts it. Every request is admitted and no vault is built, so a
// cfg configuring inbound auth or encryption is rejected rather than silently
// exercised without them; use [StartApp] for those.
func Start(t *testing.T, cfg *config.Config) *Fixture {
	t.Helper()

	require.Nil(t, cfg.Auth, "Start admits every request; use StartApp to exercise inbound auth")
	require.Nil(t, cfg.Encryption.Default, "Start builds no vault; use StartApp to exercise encryption")

	applyDefaults(cfg)

	pool := connect.NewPool()
	t.Cleanup(func() { _ = pool.Close() })

	dp, err := dataplane.New(t.Context(), cfg,
		dataplane.WithExtractor(protoutil.NewExtractor(protoregistry.GlobalFiles, protoregistry.GlobalTypes)),
		dataplane.WithTranslator(protoutil.NewTranslator(protoregistry.GlobalFiles)),
		dataplane.WithPool(pool),
		dataplane.WithMetrics(metrics.New("test", promauto.With(prometheus.NewRegistry()))),
		dataplane.WithAllowlist(config.NewAllowlist(cfg)),
		dataplane.WithAuth(auth.AdmitAll()),
		dataplane.WithLogger(logger.NewNoopLogger()),
	)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(t.Context(), lifecycleTimeout)
	defer cancel()
	require.NoError(t, dp.Start(ctx))

	// Only Stop stops a plane: the context handed to New drives the health
	// check and nothing else. t.Context is already cancelled by the time
	// cleanups run, and a shutdown must not inherit that.
	t.Cleanup(func() { requireCleanStop(t, dp.Stop(stopContext(t))) })

	return newFixture(t, dp)
}

// StartApp assembles and starts the whole production module graph around cfg,
// so config-driven collaborators the dataplane cannot build for itself, an
// inbound authenticator and an encryption vault, come from the same modules
// production uses. Prefer [Start] when a test needs neither.
func StartApp(t *testing.T, cfg *config.Config) *Fixture {
	t.Helper()

	applyDefaults(cfg)

	// A private registry per app: reporters register their collectors during
	// construction, and Prometheus rejects a duplicate registration, so a
	// shared registry would fail the second parallel test to build a plane.
	reg := prometheus.NewRegistry()

	var dp *dataplane.Dataplane
	app := fx.New(
		fx.Supply(fx.Annotate(t.Context(), fx.As(new(context.Context)))),
		fx.Supply(cfg),
		fx.Supply(fx.Annotate("127.0.0.1:0", metrics.AddrTag)),
		fx.Supply(fx.Annotate("test", metrics.NamespaceTag)),
		fx.Provide(
			func() logger.Logger { return logger.NewNoopLogger() },
			func() prometheus.Gatherer { return reg },
			func() prometheus.Registerer { return reg },
		),
		api.Module,
		auth.Module,
		connect.Module,
		dataplane.Module,
		kms.Module,
		metrics.Module,
		protoutil.Module,
		// config.Module's other half loads a file, which these tests skip in
		// favour of supplying a Config; this is the allowlist provider it would
		// otherwise contribute.
		fx.Provide(config.NewAllowlist),
		fx.Populate(&dp),
		fx.NopLogger,
	)
	require.NoError(t, app.Err())

	ctx, cancel := context.WithTimeout(t.Context(), lifecycleTimeout)
	defer cancel()
	require.NoError(t, app.Start(ctx))

	t.Cleanup(func() {
		stopCtx, stopCancel := context.WithTimeout(stopContext(t), lifecycleTimeout)
		defer stopCancel()

		requireCleanStop(t, app.Stop(stopCtx))
	})

	return newFixture(t, dp)
}

// Addr is the address the gateway is accepting on.
func (f *Fixture) Addr() string { return f.dp.Addr().String() }

// Client is a WorkflowService client on the gateway connection.
func (f *Fixture) Client() workflowservice.WorkflowServiceClient {
	return workflowservice.NewWorkflowServiceClient(f.conn)
}

// Context returns a context for one request, bounded by a deadline so a call
// that never completes fails the test rather than hanging it.
func (f *Fixture) Context() context.Context {
	ctx, cancel := context.WithTimeout(f.t.Context(), requestTimeout)
	f.t.Cleanup(cancel)

	return ctx
}

// UpstreamConn is a client connection to the named upstream's own unix socket,
// the path a local worker bypassing the gateway would dial.
func (f *Fixture) UpstreamConn(name string) *grpc.ClientConn {
	f.t.Helper()

	path, err := f.dp.SocketPath(name)
	require.NoError(f.t, err)

	return DialUnix(f.t, path)
}

// applyDefaults fills in the fields every case would otherwise repeat. Routing
// is deliberately untouched: an empty DefaultUpstream is indistinguishable from
// an unset one, so filling it would quietly make a test of the unroutable path
// unable to fail.
func applyDefaults(cfg *config.Config) {
	if cfg.Listen.HostPort == "" {
		cfg.Listen.HostPort = "127.0.0.1:0"
	}

	// Load applies this default when parsing YAML, which these tests skip.
	// Admit everything forwardable so admission never stands in for the
	// behaviour under test; it has its own coverage in internal/services and
	// internal/router.
	if len(cfg.AllowedServices) == 0 {
		cfg.AllowedServices = config.Services(services.Known())
	}
}

// newFixture dials the running gateway. gRPC connects lazily, so this opens
// nothing until the first request.
func newFixture(t *testing.T, dp *dataplane.Dataplane) *Fixture {
	t.Helper()

	require.NotNil(t, dp.Addr(), "the gateway must be accepting once Start returns")

	conn, err := grpc.NewClient(dp.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })

	return &Fixture{t: t, dp: dp, conn: conn}
}

// stopContext returns a context for shutdown that does not inherit the test's
// cancellation, which has already fired by the time cleanups run.
func stopContext(t *testing.T) context.Context {
	return context.WithoutCancel(t.Context())
}

// requireCleanStop fails the test when shutdown reported an error, so a leaked
// listener or a socket that would not close is caught by every case rather than
// only by the one test that stops a plane by hand. It reports rather than
// aborts: a cleanup that calls FailNow skips the cleanups still queued behind
// it.
func requireCleanStop(t *testing.T, err error) {
	t.Helper()

	if err != nil {
		t.Errorf("shutdown failed: %v", err)
	}
}
