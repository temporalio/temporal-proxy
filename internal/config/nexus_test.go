package config_test

import (
	"strings"
	"testing"

	"github.com/goccy/go-yaml"
	"github.com/stretchr/testify/require"

	"go.temporal.io/proxy/internal/config"
	"go.temporal.io/proxy/internal/services"
	"go.temporal.io/proxy/pkg/testutil"
)

// nexusGuard is the only error a valid http.standaloneNexus config produces until the
// connector is served. Once it is, the valid rows expect no error at all.
const nexusGuard = "http: standaloneNexus: not yet supported"

// nexusConfig returns a config with two connectors on a loopback Standalone
// Nexus listener that passes every rule but the guard.
func nexusConfig() *config.Config {
	return &config.Config{
		Listen:    config.ListenConfig{HostPort: ":7233"},
		Upstreams: []config.Upstream{{Name: "primary", Listen: config.ListenConfig{HostPort: "127.0.0.1:7233"}}},
		Routing: config.Routing{Rules: []config.RoutingRule{{
			Upstream: "primary",
			Match:    config.RoutingMatch{Namespace: "nexus-*"},
		}}},
		HTTP: config.HTTP{
			StandaloneNexus: config.StandaloneNexus{
				Listen: config.ListenConfig{HostPort: "127.0.0.1:8446", Insecure: true},
				Connectors: []config.NexusConnector{
					{
						Name:       "partners",
						PathPrefix: "/nexus/partners",
						Namespace:  "nexus-partners",
					},
					{
						Name:       "internal-apps",
						PathPrefix: "/nexus/internal",
						Namespace:  "nexus-internal",
					},
				},
			},
		},
	}
}

func TestNexusValidate(t *testing.T) {
	t.Parallel()

	certFile, keyFile := testutil.GenerateSelfSignedCert(t)
	token := func() *config.AuthConfig {
		return &config.AuthConfig{StaticToken: &config.StaticTokenConfig{Token: "s3cret"}}
	}

	tests := []struct {
		name    string
		mutate  func(c *config.Config)
		wantErr string
	}{
		{
			name:   "two connectors on loopback are valid",
			mutate: func(*config.Config) {},
		},
		{
			name: "no connectors pass the guard",
			mutate: func(c *config.Config) {
				c.HTTP.StandaloneNexus = config.StandaloneNexus{}
			},
			wantErr: "-",
		},
		{
			name: "no connectors leave the listener unchecked",
			mutate: func(c *config.Config) {
				c.HTTP.StandaloneNexus = config.StandaloneNexus{Listen: config.ListenConfig{HostPort: "not a hostport"}}
			},
			wantErr: "-",
		},
		{
			name: "a missing hostPort is rejected",
			mutate: func(c *config.Config) {
				c.HTTP.StandaloneNexus.Listen.HostPort = ""
			},
			wantErr: "http.standaloneNexus: hostPort: is required",
		},
		{
			name: "the shared listener on another port is valid",
			mutate: func(c *config.Config) {
				c.HTTP.Listen = config.ListenConfig{HostPort: "127.0.0.1:8445", Insecure: true}
				c.HTTP.CodecServer.Enabled = true
			},
		},
		{
			name: "the shared listener's hostPort is rejected",
			mutate: func(c *config.Config) {
				c.HTTP.Listen = config.ListenConfig{HostPort: "127.0.0.1:8446", Insecure: true}
				c.HTTP.CodecServer.Enabled = true
			},
			wantErr: "http.standaloneNexus: hostPort: must differ from http.hostPort",
		},
		{
			name: "the shared listener's hostPort is free while it serves nothing",
			mutate: func(c *config.Config) {
				c.HTTP.Listen = config.ListenConfig{HostPort: "127.0.0.1:8446", Insecure: true}
			},
		},
		{
			name: "port zero on both listeners does not collide",
			mutate: func(c *config.Config) {
				c.HTTP.Listen = config.ListenConfig{HostPort: "127.0.0.1:0", Insecure: true}
				c.HTTP.CodecServer.Enabled = true
				c.HTTP.StandaloneNexus.Listen.HostPort = "127.0.0.1:0"
			},
		},
		{
			name: "a default upstream routes any namespace",
			mutate: func(c *config.Config) {
				c.Routing = config.Routing{DefaultUpstream: "primary"}
			},
		},
		{
			name: "an earlier metadata rule is skipped for a later namespace rule",
			mutate: func(c *config.Config) {
				c.Routing.Rules = append([]config.RoutingRule{{
					Upstream: "primary",
					Match:    config.RoutingMatch{Metadata: map[string]string{"x-tenant": "*"}},
				}}, c.Routing.Rules...)
			},
		},
		{
			name: "off loopback with auth and tls is valid beside a plaintext loopback shared listener",
			mutate: func(c *config.Config) {
				c.HTTP.Listen = config.ListenConfig{HostPort: "127.0.0.1:8445", Insecure: true}
				c.HTTP.CodecServer.Enabled = true
				c.HTTP.StandaloneNexus.Listen = config.ListenConfig{
					HostPort: "0.0.0.0:8446",
					TLS:      &config.TLSConfig{Cert: certFile, Key: keyFile},
				}
				c.HTTP.StandaloneNexus.Connectors[0].Auth = token()
				c.HTTP.StandaloneNexus.Connectors[1].Auth = token()
			},
		},
		{
			name: "duplicate connector names are rejected",
			mutate: func(c *config.Config) {
				c.HTTP.StandaloneNexus.Connectors[1].Name = "partners"
			},
			wantErr: "http.standaloneNexus: connectors[name]: contains duplicate value: partners",
		},
		{
			name: "duplicate path prefixes are rejected",
			mutate: func(c *config.Config) {
				c.HTTP.StandaloneNexus.Connectors[1].PathPrefix = "/nexus/partners"
			},
			wantErr: "http.standaloneNexus: connectors[pathPrefix]: contains duplicate value: /nexus/partners",
		},
		{
			name: "a path prefix nested under another connector's is rejected",
			mutate: func(c *config.Config) {
				c.HTTP.StandaloneNexus.Connectors[1].PathPrefix = "/nexus/partners/internal"
			},
			wantErr: `http.standaloneNexus.connectors[1]: pathPrefix: overlaps connectors[0] pathPrefix "/nexus/partners"`,
		},
		{
			name: "a path prefix enclosing another connector's is rejected",
			mutate: func(c *config.Config) {
				c.HTTP.StandaloneNexus.Connectors[1].PathPrefix = "/nexus/partners"
				c.HTTP.StandaloneNexus.Connectors[0].PathPrefix = "/nexus/partners/a/b"
			},
			wantErr: `http.standaloneNexus.connectors[1]: pathPrefix: overlaps connectors[0] pathPrefix "/nexus/partners/a/b"`,
		},
		{
			name: "a sibling sharing a string prefix is not an overlap",
			mutate: func(c *config.Config) {
				c.HTTP.StandaloneNexus.Connectors[1].PathPrefix = "/nexus/partnersx"
			},
		},
		{
			name: "routes the shared listener serves are free here",
			mutate: func(c *config.Config) {
				c.HTTP.StandaloneNexus.Connectors[0].PathPrefix = "/orders/decode"
				c.HTTP.StandaloneNexus.Connectors[1].PathPrefix = "/debug/pprof/nexus"
			},
		},
		{
			name: "a single segment is valid",
			mutate: func(c *config.Config) {
				c.HTTP.StandaloneNexus.Connectors[0].PathPrefix = "/partners"
			},
		},
		{
			name: "a missing path prefix is rejected",
			mutate: func(c *config.Config) {
				c.HTTP.StandaloneNexus.Connectors[0].PathPrefix = ""
			},
			wantErr: "http.standaloneNexus.connectors[0]: pathPrefix: is required",
		},
		{
			name: "a relative path prefix is rejected",
			mutate: func(c *config.Config) {
				c.HTTP.StandaloneNexus.Connectors[0].PathPrefix = "nexus/partners"
			},
			wantErr: "http.standaloneNexus.connectors[0]: pathPrefix: must be an absolute path",
		},
		{
			name: "the root path prefix is rejected",
			mutate: func(c *config.Config) {
				c.HTTP.StandaloneNexus.Connectors[0].PathPrefix = "/"
			},
			wantErr: "http.standaloneNexus.connectors[0]: pathPrefix: must not be /",
		},
		{
			name: "a trailing slash is rejected",
			mutate: func(c *config.Config) {
				c.HTTP.StandaloneNexus.Connectors[0].PathPrefix = "/nexus/partners/"
			},
			wantErr: "http.standaloneNexus.connectors[0]: pathPrefix: must be a clean path",
		},
		{
			name: "a dot segment is rejected",
			mutate: func(c *config.Config) {
				c.HTTP.StandaloneNexus.Connectors[0].PathPrefix = "/nexus/../partners"
			},
			wantErr: "http.standaloneNexus.connectors[0]: pathPrefix: must be a clean path",
		},
		{
			name: "a wildcard is rejected",
			mutate: func(c *config.Config) {
				c.HTTP.StandaloneNexus.Connectors[0].PathPrefix = "/nexus/{name}"
			},
			wantErr: "http.standaloneNexus.connectors[0]: pathPrefix: must be a literal path",
		},
		{
			name: "a missing connector name is rejected",
			mutate: func(c *config.Config) {
				c.HTTP.StandaloneNexus.Connectors[0].Name = ""
			},
			wantErr: "http.standaloneNexus.connectors[0]: name: is required",
		},
		{
			name: "a missing namespace is rejected",
			mutate: func(c *config.Config) {
				c.HTTP.StandaloneNexus.Connectors[0].Namespace = ""
			},
			wantErr: "http.standaloneNexus.connectors[0]: namespace: is required",
		},
		{
			name: "a namespace no rule matches and no default upstream is rejected",
			mutate: func(c *config.Config) {
				c.HTTP.StandaloneNexus.Connectors[0].Namespace = "orders"
			},
			wantErr: `http.standaloneNexus.connectors[0]: namespace: "orders" routes to no upstream`,
		},
		{
			name: "a rule matching on metadata never routes a connector",
			mutate: func(c *config.Config) {
				c.Routing.Rules[0].Match.Metadata = map[string]string{"x-tenant": "*"}
			},
			wantErr: `http.standaloneNexus.connectors[0]: namespace: "nexus-partners" routes to no upstream`,
		},
		{
			name: "the system upstream does not route a connector",
			mutate: func(c *config.Config) {
				c.Routing = config.Routing{SystemUpstream: "primary"}
			},
			wantErr: `http.standaloneNexus.connectors[0]: namespace: "nexus-partners" routes to no upstream`,
		},
		{
			name: "a negative max request body size is rejected",
			mutate: func(c *config.Config) {
				c.HTTP.StandaloneNexus.Connectors[0].MaxRequestBodySize = -1
			},
			wantErr: "http.standaloneNexus.connectors[0]: maxRequestBodySize",
		},
		{
			name: "a connector without auth beyond loopback is rejected",
			mutate: func(c *config.Config) {
				c.HTTP.StandaloneNexus.Listen = config.ListenConfig{
					HostPort: "0.0.0.0:8446",
					TLS:      &config.TLSConfig{Cert: certFile, Key: keyFile},
				}
				c.HTTP.StandaloneNexus.Connectors[0].Auth = token()
			},
			wantErr: "http.standaloneNexus.connectors[1]: auth: auth is required unless hostPort is loopback",
		},
		{
			name: "auth beyond loopback requires tls on the Standalone Nexus listener",
			mutate: func(c *config.Config) {
				c.HTTP.Listen = config.ListenConfig{
					HostPort: "0.0.0.0:8445",
					TLS:      &config.TLSConfig{Cert: certFile, Key: keyFile},
				}
				c.HTTP.CodecServer = config.CodecServer{Enabled: true, Auth: token()}
				c.HTTP.StandaloneNexus.Listen = config.ListenConfig{HostPort: "0.0.0.0:8446", Insecure: true}
				c.HTTP.StandaloneNexus.Connectors[0].Auth = token()
				c.HTTP.StandaloneNexus.Connectors[1].Auth = token()
			},
			wantErr: "http.standaloneNexus: tls: tls is required when auth is configured and hostPort is not loopback",
		},
		{
			name: "a connector's auth is validated",
			mutate: func(c *config.Config) {
				c.HTTP.StandaloneNexus.Connectors[0].Auth = &config.AuthConfig{StaticToken: &config.StaticTokenConfig{}}
			},
			wantErr: "http.standaloneNexus.connectors[0].auth.staticToken: token: is required",
		},
		{
			name: "a connector's external auth must name an extension server",
			mutate: func(c *config.Config) {
				c.HTTP.StandaloneNexus.Connectors[0].Auth = &config.AuthConfig{
					External: &config.ExternalAuthConfig{Name: "nope"},
				}
			},
			wantErr: "http.standaloneNexus.connectors[0].auth.external: name: unknown extension server: nope",
		},
		{
			name: "connectors require WorkflowService among the allowed services",
			mutate: func(c *config.Config) {
				c.AllowedServices = config.Services{services.OperatorService}
			},
			wantErr: "http: standaloneNexus: requires allowedServices to include " + services.WorkflowService,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			cfg := nexusConfig()
			tc.mutate(cfg)

			err := cfg.Prepare()
			switch tc.wantErr {
			case "":
				require.EqualError(t, err, nexusGuard)
			case "-":
				require.NoError(t, err)
			default:
				require.ErrorContains(t, err, tc.wantErr)
			}
		})
	}
}

// nexusYAML is adapted from the configuration example in
// rfc/nexus-http-connector.md.
const nexusYAML = `hostPort: :7233
upstreams:
  - name: primary
    hostPort: 127.0.0.1:7233
routing:
  default: primary
http:
  standaloneNexus:
    hostPort: 127.0.0.1:8446
    insecure: true
    connectors:
      - name: partners
        pathPrefix: /nexus/partners
        namespace: nexus-partners
        identity: temporal-proxy-partners
        maxRequestBodySize: 4MiB
        auth:
          staticToken:
            token: s3cret
      - name: internal-apps
        pathPrefix: /nexus/internal
        namespace: nexus-internal
`

func TestLoadNexus(t *testing.T) {
	t.Parallel()

	var cfg config.Config
	require.NoError(t, yaml.Unmarshal([]byte(nexusYAML), &cfg))

	require.Equal(t, config.StandaloneNexus{
		Listen: config.ListenConfig{HostPort: "127.0.0.1:8446", Insecure: true},
		Connectors: []config.NexusConnector{
			{
				Name:               "partners",
				PathPrefix:         "/nexus/partners",
				Namespace:          "nexus-partners",
				Identity:           "temporal-proxy-partners",
				MaxRequestBodySize: 4 << 20,
				Auth:               &config.AuthConfig{StaticToken: &config.StaticTokenConfig{Token: "s3cret"}},
			},
			{
				Name:       "internal-apps",
				PathPrefix: "/nexus/internal",
				Namespace:  "nexus-internal",
			},
		},
	}, cfg.HTTP.StandaloneNexus)

	require.EqualError(t, cfg.Prepare(), nexusGuard)

	_, err := config.Load(strings.NewReader(nexusYAML))
	require.EqualError(t, err, nexusGuard)
}
