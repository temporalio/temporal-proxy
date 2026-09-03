package config_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/temporalio/temporal-proxy/internal/config"
	"github.com/temporalio/temporal-proxy/pkg/testutil"
)

func TestExtensionServer_Validate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		server     *config.ExtensionServer
		wantTuples [][2]string
	}{
		{
			name: "valid name and hostPort",
			server: &config.ExtensionServer{
				Name:   "audit",
				Listen: config.ListenConfig{HostPort: "127.0.0.1:9090"},
			},
		},
		{
			name: "missing name surfaces required error",
			server: &config.ExtensionServer{
				Listen: config.ListenConfig{HostPort: "127.0.0.1:9090"},
			},
			wantTuples: [][2]string{{"", "name"}},
		},
		{
			name: "invalid hostPort is rejected",
			server: &config.ExtensionServer{
				Name:   "audit",
				Listen: config.ListenConfig{HostPort: "not-a-host-port"},
			},
			wantTuples: [][2]string{{"", "hostPort"}},
		},
		{
			name:       "missing name and hostPort aggregate",
			server:     &config.ExtensionServer{},
			wantTuples: [][2]string{{"", "name"}, {"", "hostPort"}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assertTuples(t, tt.server.Validate(), tt.wantTuples)
		})
	}
}

func TestExtensionServer_ValidateRejectsTemplatedHostPort(t *testing.T) {
	t.Parallel()

	// Unlike Upstream, an extension server is dialed at a fixed address with no
	// per-request template resolution, so a template is rejected outright. It
	// is not enough to lean on the host:port check: a template carrying a
	// literal port parses as a valid host:port and would otherwise be accepted.
	tests := []struct {
		name     string
		hostPort string
	}{
		{
			name:     "template with a literal port",
			hostPort: "{{ .RemoteNamespace }}.acme.cloud:9090",
		},
		{
			name:     "template without a port",
			hostPort: "{{ .RemoteNamespace }}.acme.cloud",
		},
		{
			name:     "template in the port position",
			hostPort: "audit.internal:{{ .Port }}",
		},
		{
			name:     "template spanning the whole value",
			hostPort: "{{ .ExtensionHostPort }}",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			s := &config.ExtensionServer{
				Name:   "audit",
				Listen: config.ListenConfig{HostPort: tt.hostPort},
			}

			err := s.Validate()
			assertTuples(t, err, [][2]string{{"", "hostPort"}})
			require.ErrorContains(t, err, "templates are not resolved for extension servers")
		})
	}
}

func TestExtensionServer_ValidateCredentialsRequireTLS(t *testing.T) {
	t.Parallel()

	base := func() config.ExtensionServer {
		return config.ExtensionServer{
			Name:        "audit",
			Listen:      config.ListenConfig{HostPort: "audit.internal:9090"},
			Credentials: &config.CredentialConfig{Static: &config.StaticCredentialConfig{APIKey: "k"}},
		}
	}

	t.Run("credentials without tls is rejected", func(t *testing.T) {
		t.Parallel()

		s := base()
		require.ErrorContains(t, s.Validate(), "requires TLS")
	})

	t.Run("credentials with tls is accepted", func(t *testing.T) {
		t.Parallel()

		s := base()
		s.Listen.TLS = &config.TLSConfig{ServerName: "audit.internal"}
		require.NoError(t, s.Validate())
	})
}

func TestExtensionServer_ValidateOutboundTLS(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		tls     func(t *testing.T) *config.TLSConfig
		wantErr string
	}{
		{
			name: "server name only is valid (system roots)",
			tls: func(*testing.T) *config.TLSConfig {
				return &config.TLSConfig{ServerName: "audit.internal"}
			},
		},
		{
			name: "CA plus client key pair is valid (mutual TLS)",
			tls: func(t *testing.T) *config.TLSConfig {
				caFile, certFile, keyFile := testutil.GenerateMTLSCerts(t)
				return &config.TLSConfig{CA: caFile, Cert: certFile, Key: keyFile, ServerName: "localhost"}
			},
		},
		{
			name: "cert without key is rejected",
			tls: func(t *testing.T) *config.TLSConfig {
				_, certFile, _ := testutil.GenerateMTLSCerts(t)
				return &config.TLSConfig{Cert: certFile}
			},
			wantErr: "certificate and key must be set together",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			s := config.ExtensionServer{
				Name:   "audit",
				Listen: config.ListenConfig{HostPort: "audit.internal:9090", TLS: tt.tls(t)},
			}

			err := s.Validate()
			if tt.wantErr == "" {
				require.NoError(t, err)
				return
			}

			require.ErrorContains(t, err, tt.wantErr)
		})
	}
}

func TestExtensionServerList_Validate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		list       config.ExtensionServerList
		wantTuples [][2]string
	}{
		{
			name: "empty list yields no error",
			list: config.ExtensionServerList{},
		},
		{
			name: "nil list yields no error",
			list: nil,
		},
		{
			name: "single valid server yields no error",
			list: config.ExtensionServerList{
				{Name: "audit", Listen: config.ListenConfig{HostPort: "127.0.0.1:9090"}},
			},
		},
		{
			name: "distinct names and hostPorts yield no error",
			list: config.ExtensionServerList{
				{Name: "audit", Listen: config.ListenConfig{HostPort: "127.0.0.1:9090"}},
				{Name: "quota", Listen: config.ListenConfig{HostPort: "127.0.0.1:9091"}},
			},
		},
		{
			name: "duplicate names surface on the collection field",
			list: config.ExtensionServerList{
				{Name: "dup", Listen: config.ListenConfig{HostPort: "127.0.0.1:9090"}},
				{Name: "dup", Listen: config.ListenConfig{HostPort: "127.0.0.1:9091"}},
			},
			wantTuples: [][2]string{{"", "[name]"}},
		},
		{
			name: "duplicate hostPorts surface on the collection field",
			list: config.ExtensionServerList{
				{Name: "audit", Listen: config.ListenConfig{HostPort: "127.0.0.1:9090"}},
				{Name: "quota", Listen: config.ListenConfig{HostPort: "127.0.0.1:9090"}},
			},
			wantTuples: [][2]string{{"", "[hostPort]"}},
		},
		{
			name: "element failure is stamped with its index",
			list: config.ExtensionServerList{
				{Name: "audit", Listen: config.ListenConfig{HostPort: "127.0.0.1:9090"}},
				{Listen: config.ListenConfig{HostPort: "127.0.0.1:9091"}},
			},
			wantTuples: [][2]string{{"[1]", "name"}},
		},
		{
			name: "collection and element failures aggregate",
			list: config.ExtensionServerList{
				{Name: "dup", Listen: config.ListenConfig{HostPort: "127.0.0.1:9090"}},
				{Name: "dup", Listen: config.ListenConfig{HostPort: "nope"}},
			},
			wantTuples: [][2]string{{"", "[name]"}, {"[1]", "hostPort"}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assertTuples(t, tt.list.Validate(), tt.wantTuples)
		})
	}
}

func TestConfig_Validate_ExtensionServers(t *testing.T) {
	t.Parallel()

	base := func(servers config.ExtensionServerList) *config.Config {
		return &config.Config{
			Listen:           config.ListenConfig{HostPort: ":8080"},
			ExtensionServers: servers,
			Metrics:          defaultMetrics(),
			Upstreams: config.UpstreamList{
				{Name: "primary", Listen: config.ListenConfig{HostPort: "127.0.0.1:7233"}},
			},
		}
	}

	tests := []struct {
		name       string
		servers    config.ExtensionServerList
		wantTuples [][2]string
	}{
		{
			name:    "absent extension servers are optional",
			servers: nil,
		},
		{
			name: "valid extension servers yield no error",
			servers: config.ExtensionServerList{
				{Name: "audit", Listen: config.ListenConfig{HostPort: "127.0.0.1:9090"}},
			},
		},
		{
			name: "element failure carries the indexed extensionServers subject",
			servers: config.ExtensionServerList{
				{Listen: config.ListenConfig{HostPort: "127.0.0.1:9090"}},
			},
			wantTuples: [][2]string{{"extensionServers[0]", "name"}},
		},
		{
			name: "duplicate names carry the extensionServers[name] field and no subject",
			servers: config.ExtensionServerList{
				{Name: "dup", Listen: config.ListenConfig{HostPort: "127.0.0.1:9090"}},
				{Name: "dup", Listen: config.ListenConfig{HostPort: "127.0.0.1:9091"}},
			},
			wantTuples: [][2]string{{"", "extensionServers[name]"}},
		},
		{
			name: "duplicate hostPorts carry the extensionServers[hostPort] field",
			servers: config.ExtensionServerList{
				{Name: "audit", Listen: config.ListenConfig{HostPort: "127.0.0.1:9090"}},
				{Name: "quota", Listen: config.ListenConfig{HostPort: "127.0.0.1:9090"}},
			},
			wantTuples: [][2]string{{"", "extensionServers[hostPort]"}},
		},
		{
			name: "an extension server may reuse an upstream hostPort",
			servers: config.ExtensionServerList{
				{Name: "audit", Listen: config.ListenConfig{HostPort: "127.0.0.1:7233"}},
			},
		},
		{
			name: "a nested TLS failure composes onto the indexed subject",
			servers: config.ExtensionServerList{
				{
					Name: "audit",
					Listen: config.ListenConfig{
						HostPort: "127.0.0.1:9090",
						TLS:      &config.TLSConfig{Cert: "/nope/cert.pem"},
					},
				},
			},
			wantTuples: [][2]string{{"extensionServers[0].tls", "cert"}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assertTuples(t, base(tt.servers).Validate(), tt.wantTuples)
		})
	}
}
