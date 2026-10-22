package api_test

import (
	"context"
	"maps"
	"net"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/fx"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"go.temporal.io/proxy/internal/api"
	"go.temporal.io/proxy/internal/config"
	"go.temporal.io/proxy/internal/transport/connect"
	"go.temporal.io/proxy/internal/transport/creds"
	"go.temporal.io/proxy/pkg/testutil"
)

// credentialRecorder is a [creds.Reporter] that keeps the credential label of
// every expiry it is told about.
type credentialRecorder struct {
	mu    sync.Mutex
	names []string
}

func (c *credentialRecorder) Attempt(string, error) {}

func (c *credentialRecorder) NotAfter(credential, _ string, _ time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.names = append(c.names, credential)
}

func (c *credentialRecorder) credentials() []string {
	c.mu.Lock()
	defer c.mu.Unlock()

	return append([]string(nil), c.names...)
}

// buildModule wires api.Module against the supplied config and pool, returning
// the extension server connections and whatever error fx reported. fx.Populate
// demands the value, so a dial failure surfaces from fx.New.
func buildModule(t *testing.T, cfg *config.Config, pool *connect.Pool) (api.Connections, error) {
	t.Helper()

	return buildModuleWithReloader(t, cfg, pool, creds.NewReloader())
}

// buildModuleWithReloader is [buildModule] with the extension servers' TLS
// material registered with r.
func buildModuleWithReloader(
	t *testing.T,
	cfg *config.Config,
	pool *connect.Pool,
	r *creds.Reloader,
) (api.Connections, error) {
	t.Helper()

	var out api.Connections
	app := fx.New(
		fx.Supply(cfg, pool, r),
		api.Module,
		fx.Populate(&out),
		fx.NopLogger,
	)

	return out, app.Err()
}

// deadAddr returns a loopback address with nothing behind it, by taking a port
// from the kernel and immediately giving it back.
func deadAddr(t *testing.T) string {
	t.Helper()

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	addr := lis.Addr().String()
	require.NoError(t, lis.Close())

	return addr
}

func extensionServers(servers ...config.ExtensionServer) *config.Config {
	return &config.Config{
		Listen:           config.ListenConfig{HostPort: ":8080"},
		ExtensionServers: servers,
		Upstreams: config.UpstreamList{
			{Name: "primary", Listen: config.ListenConfig{HostPort: "127.0.0.1:7233"}},
		},
	}
}

func TestModuleFailsStartWhenExtensionServerUnreachable(t *testing.T) {
	t.Parallel()

	pool := connect.NewPool()
	t.Cleanup(func() { require.NoError(t, pool.Close()) })

	var out api.Connections
	app := fx.New(
		fx.Supply(
			extensionServers(config.ExtensionServer{
				Name:   "audit",
				Listen: config.ListenConfig{HostPort: deadAddr(t)},
			}),
			pool,
			creds.NewReloader(),
		),
		api.Module,
		fx.Populate(&out),
		fx.NopLogger,
	)

	// An unreachable server is valid configuration, so it survives construction
	// (grpc.NewClient does not dial) and fails when the connection is opened on
	// start.
	require.NoError(t, app.Err())

	startCtx, cancel := context.WithTimeout(t.Context(), 500*time.Millisecond)
	defer cancel()

	require.ErrorContains(t, app.Start(startCtx), "extension server connection not ready")
}

func TestModule_RegistersExtensionCredential(t *testing.T) {
	t.Parallel()

	ca, _, _ := testutil.GenerateMTLSCerts(t)

	rec := &credentialRecorder{}
	r := creds.NewReloader(creds.WithReporter(rec))

	pool := connect.NewPool()
	t.Cleanup(func() { require.NoError(t, pool.Close()) })

	_, err := buildModuleWithReloader(t, extensionServers(config.ExtensionServer{
		Name:   "kms",
		Listen: config.ListenConfig{HostPort: "127.0.0.1:9090", TLS: &config.TLSConfig{CA: ca}},
	}), pool, r)
	require.NoError(t, err)

	require.NoError(t, r.Start(t.Context()))
	t.Cleanup(func() { require.NoError(t, r.Stop(context.WithoutCancel(t.Context()))) })

	require.Equal(t, []string{"extension/kms"}, rec.credentials())
}

func TestModuleBuildsAConnPerExtensionServer(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		servers   []config.ExtensionServer
		wantNames []string
	}{
		{
			name:      "no extension servers yields no connections",
			wantNames: []string{},
		},
		{
			name: "one server is keyed by its name",
			servers: []config.ExtensionServer{
				{Name: "audit", Listen: config.ListenConfig{HostPort: "127.0.0.1:9090"}},
			},
			wantNames: []string{"audit"},
		},
		{
			name: "every server gets its own entry",
			servers: []config.ExtensionServer{
				{Name: "audit", Listen: config.ListenConfig{HostPort: "127.0.0.1:9090"}},
				{Name: "quota", Listen: config.ListenConfig{HostPort: "127.0.0.1:9091"}},
				{Name: "hsm", Listen: config.ListenConfig{HostPort: "127.0.0.1:9092"}},
			},
			wantNames: []string{"audit", "quota", "hsm"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := buildModule(t, extensionServers(tt.servers...), connect.NewPool())
			require.NoError(t, err)
			require.ElementsMatch(t, tt.wantNames, slices.Collect(maps.Keys(got)))

			for name, conn := range got {
				require.NotNil(t, conn, "no connection for %q", name)
			}
		})
	}
}

func TestModuleGivesEachServerADistinctConn(t *testing.T) {
	t.Parallel()

	// Several keys may address one extension server and share its connection,
	// but two different servers must never share one.
	got, err := buildModule(t, extensionServers(
		config.ExtensionServer{Name: "audit", Listen: config.ListenConfig{HostPort: "127.0.0.1:9090"}},
		config.ExtensionServer{Name: "quota", Listen: config.ListenConfig{HostPort: "127.0.0.1:9091"}},
	), connect.NewPool())
	require.NoError(t, err)
	require.NotSame(t, got["audit"], got["quota"])
}

func TestModulePoolsConnectionsUnderANamespacedKey(t *testing.T) {
	t.Parallel()

	pool := connect.NewPool()
	_, err := buildModule(t, extensionServers(
		config.ExtensionServer{Name: "audit", Listen: config.ListenConfig{HostPort: "127.0.0.1:9090"}},
	), pool)
	require.NoError(t, err)

	// The connection is created during construction, under a key distinct from
	// the bare dial address, so it cannot be handed to a caller resolving that
	// address.
	conn, err := pool.Conn("extension:127.0.0.1:9090")
	require.NoError(t, err)
	require.NotNil(t, conn)

	_, err = pool.Conn("127.0.0.1:9090")
	require.ErrorIs(t, err, connect.ErrKeyNotFound)
}

func TestModuleDoesNotCollideWithAnUpstreamOnTheSameHostPort(t *testing.T) {
	t.Parallel()

	const hostPort = "127.0.0.1:7233"

	// Config permits an extension server to sit on an upstream's hostPort, and
	// the pool is shared between them. Stand in for the upstream by seeding the
	// pool the way a static resolver would, keyed by the bare address.
	pool := connect.NewPool()
	upstreamConn, err := grpc.NewClient(hostPort, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	require.NoError(t, pool.Set(hostPort, upstreamConn))

	_, err = buildModule(t, extensionServers(
		config.ExtensionServer{Name: "audit", Listen: config.ListenConfig{HostPort: hostPort}},
	), pool)
	require.NoError(t, err)

	extensionConn, err := pool.Conn("extension:" + hostPort)
	require.NoError(t, err)

	// Distinct entries: the extension server did not inherit the upstream's
	// connection, which carries the upstream's TLS settings and credentials.
	require.NotSame(t, upstreamConn, extensionConn)

	got, err := pool.Conn(hostPort)
	require.NoError(t, err)
	require.Same(t, upstreamConn, got, "the upstream entry must be left untouched")
}
