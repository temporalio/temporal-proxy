package config_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/temporalio/temporal-proxy/internal/config"
)

func TestHTTPValidate(t *testing.T) {
	t.Parallel()

	withAuth := func() *config.AuthConfig {
		return &config.AuthConfig{StaticToken: &config.StaticTokenConfig{Token: "s3cret"}}
	}

	enabled := config.CodecServer{Enabled: true}

	tests := []struct {
		name    string
		h       config.HTTP
		wantErr string
	}{
		{
			name: "no enabled group is always valid",
			h:    config.HTTP{Listen: config.ListenConfig{HostPort: "not a host port"}},
		},
		{
			name: "loopback without auth is allowed",
			h: config.HTTP{
				Listen:      config.ListenConfig{HostPort: "127.0.0.1:8445", Insecure: true},
				CodecServer: enabled,
			},
		},
		{
			name: "localhost counts as loopback",
			h: config.HTTP{
				Listen:      config.ListenConfig{HostPort: "localhost:8445", Insecure: true},
				CodecServer: enabled,
			},
		},
		{
			name: "reachable without auth is rejected",
			h: config.HTTP{
				Listen:      config.ListenConfig{HostPort: "0.0.0.0:8445", Insecure: true},
				CodecServer: enabled,
			},
			wantErr: "auth is required unless hostPort is loopback",
		},
		{
			name: "every interface is not loopback",
			h: config.HTTP{
				Listen:      config.ListenConfig{HostPort: ":8445", Insecure: true},
				CodecServer: enabled,
			},
			wantErr: "auth is required unless hostPort is loopback",
		},
		{
			name: "reachable with auth needs TLS",
			h: config.HTTP{
				Listen:      config.ListenConfig{HostPort: "0.0.0.0:8445", Insecure: true},
				CodecServer: config.CodecServer{Enabled: true, Auth: withAuth()},
			},
			wantErr: "tls is required when auth is configured and hostPort is not loopback",
		},
		{
			name: "missing hostPort is rejected",
			h: config.HTTP{
				Listen:      config.ListenConfig{Insecure: true},
				CodecServer: enabled,
			},
			wantErr: "hostPort",
		},
		{
			name: "a tls cert without a key is rejected",
			h: config.HTTP{
				Listen: config.ListenConfig{
					HostPort: "127.0.0.1:8445",
					TLS:      &config.TLSConfig{Cert: "/nope.pem"},
				},
				CodecServer: enabled,
			},
			wantErr: "certificate and key must be set together",
		},
		{
			name: "a group's own rules are checked",
			h: config.HTTP{
				Listen: config.ListenConfig{HostPort: "127.0.0.1:8445", Insecure: true},
				CodecServer: config.CodecServer{
					Enabled: true,
					CORS:    config.CORSConfig{Origins: []string{"*"}},
				},
			},
			wantErr: "codecServer.cors",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			err := tc.h.Validate()
			if tc.wantErr != "" {
				require.ErrorContains(t, err, tc.wantErr)

				return
			}

			require.NoError(t, err)
		})
	}
}

func TestLoad_HTTP(t *testing.T) {
	t.Parallel()

	cfg, err := config.Load(strings.NewReader(
		"http:\n  hostPort: 127.0.0.1:8445\n  insecure: true\n" +
			"  codecServer:\n    enabled: true\n    cors:\n      origins: [http://localhost:8233]\n",
	))
	require.NoError(t, err)
	require.Equal(t, "127.0.0.1:8445", cfg.HTTP.Listen.HostPort)
	require.True(t, cfg.HTTP.Listen.Insecure)
	require.True(t, cfg.HTTP.CodecServer.Enabled)
	require.Equal(t, []string{"http://localhost:8233"}, cfg.HTTP.CodecServer.CORS.Origins)
}

// A v0.8.0 config put the codec server at the top level. Load ignores keys it
// does not know, so without this rule the block would silently turn into a
// disabled codec server.
func TestValidate_RetiredCodecServerIsRejected(t *testing.T) {
	t.Parallel()

	cfg, err := config.Load(strings.NewReader(
		"routing:\n  default: local\n" +
			"upstreams:\n  - name: local\n    hostPort: localhost:7233\n    insecure: true\n" +
			"codecServer:\n  enabled: true\n  hostPort: 127.0.0.1:8445\n  insecure: true\n",
	))
	require.NoError(t, err)
	require.ErrorContains(t, cfg.Validate(), "moved to http.codecServer")
}
