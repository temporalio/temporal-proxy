package api_test

import (
	"maps"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/fx"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/temporalio/temporal-proxy/internal/api"
	"github.com/temporalio/temporal-proxy/internal/config"
	"github.com/temporalio/temporal-proxy/internal/transport/connect"
)

// buildModule wires api.Module against the supplied config and pool, returning
// the extension server connections and whatever error fx reported. fx.Populate
// demands the value, so a dial failure surfaces from fx.New.
func buildModule(t *testing.T, cfg *config.Config, pool *connect.Pool) (api.Connections, error) {
	t.Helper()

	var out api.Connections
	app := fx.New(
		fx.Supply(cfg, pool),
		api.Module,
		fx.Populate(&out),
		fx.NopLogger,
	)

	return out, app.Err()
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

func TestModuleRejectsInvalidExtensionServers(t *testing.T) {
	t.Parallel()

	// api.Module is wired ahead of server.Module, so it cannot assume the
	// config has been validated; a bad entry must fail here rather than be
	// dialed as-is.
	tests := []struct {
		name    string
		server  config.ExtensionServer
		wantErr string
	}{
		{
			name:    "templated hostPort",
			server:  config.ExtensionServer{Name: "audit", Listen: config.ListenConfig{HostPort: "{{ .Ns }}.acme.cloud:9090"}},
			wantErr: "templates are not resolved for extension servers",
		},
		{
			name:    "missing name",
			server:  config.ExtensionServer{Listen: config.ListenConfig{HostPort: "127.0.0.1:9090"}},
			wantErr: "name",
		},
		{
			name:    "invalid hostPort",
			server:  config.ExtensionServer{Name: "audit", Listen: config.ListenConfig{HostPort: "nope"}},
			wantErr: "is not a valid host:port",
		},
		{
			name: "credentials without TLS",
			server: config.ExtensionServer{
				Name:        "audit",
				Listen:      config.ListenConfig{HostPort: "127.0.0.1:9090"},
				Credentials: &config.CredentialConfig{Static: &config.StaticCredentialConfig{APIKey: "k"}},
			},
			wantErr: "requires TLS",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := buildModule(t, extensionServers(tt.server), connect.NewPool())
			require.ErrorContains(t, err, tt.wantErr)
		})
	}
}

func TestModulePoolsConnectionsUnderANamespacedKey(t *testing.T) {
	t.Parallel()

	pool := connect.NewPool()
	_, err := buildModule(t, extensionServers(
		config.ExtensionServer{Name: "audit", Listen: config.ListenConfig{HostPort: "127.0.0.1:9090"}},
	), pool)
	require.NoError(t, err)

	// The connection is created eagerly, under a key distinct from the bare
	// dial address, so it cannot be handed to a caller resolving that address.
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

func TestModuleRejectsCollectionLevelConfigErrors(t *testing.T) {
	t.Parallel()

	// The whole list is validated, not just each entry. Duplicates would
	// otherwise collapse into one map key and silently drop a server.
	tests := []struct {
		name    string
		servers []config.ExtensionServer
		wantErr string
	}{
		{
			name: "duplicate names",
			servers: []config.ExtensionServer{
				{Name: "audit", Listen: config.ListenConfig{HostPort: "127.0.0.1:9090"}},
				{Name: "audit", Listen: config.ListenConfig{HostPort: "127.0.0.1:9091"}},
			},
			wantErr: "contains duplicate value: audit",
		},
		{
			name: "duplicate hostPorts",
			servers: []config.ExtensionServer{
				{Name: "audit", Listen: config.ListenConfig{HostPort: "127.0.0.1:9090"}},
				{Name: "quota", Listen: config.ListenConfig{HostPort: "127.0.0.1:9090"}},
			},
			wantErr: "contains duplicate value: 127.0.0.1:9090",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := buildModule(t, extensionServers(tt.servers...), connect.NewPool())
			require.ErrorContains(t, err, tt.wantErr)
		})
	}
}
