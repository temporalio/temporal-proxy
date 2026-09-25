package config_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/temporalio/temporal-proxy/internal/config"
)

func TestCodecServerValidate(t *testing.T) {
	t.Parallel()

	withAuth := func() *config.AuthConfig {
		return &config.AuthConfig{StaticToken: &config.StaticTokenConfig{Token: "s3cret"}}
	}

	tests := []struct {
		name    string
		cs      config.CodecServer
		wantErr string
	}{
		{
			name: "disabled is always valid",
			cs:   config.CodecServer{},
		},
		{
			name: "loopback without auth is allowed",
			cs: config.CodecServer{
				Enabled: true,
				Listen:  config.ListenConfig{HostPort: "127.0.0.1:8445", Insecure: true},
			},
		},
		{
			name: "localhost counts as loopback",
			cs: config.CodecServer{
				Enabled: true,
				Listen:  config.ListenConfig{HostPort: "localhost:8445", Insecure: true},
			},
		},
		{
			name: "reachable without auth is rejected",
			cs: config.CodecServer{
				Enabled: true,
				Listen:  config.ListenConfig{HostPort: "0.0.0.0:8445", Insecure: true},
			},
			wantErr: "auth is required unless hostPort is loopback",
		},
		{
			name: "every interface is not loopback",
			cs: config.CodecServer{
				Enabled: true,
				Listen:  config.ListenConfig{HostPort: ":8445", Insecure: true},
			},
			wantErr: "auth is required unless hostPort is loopback",
		},
		{
			name: "reachable with auth needs TLS",
			cs: config.CodecServer{
				Enabled: true,
				Listen:  config.ListenConfig{HostPort: "0.0.0.0:8445", Insecure: true},
				Auth:    withAuth(),
			},
			wantErr: "tls is required when auth is configured and hostPort is not loopback",
		},
		{
			name: "credentials without origins is rejected",
			cs: config.CodecServer{
				Enabled: true,
				Listen:  config.ListenConfig{HostPort: "127.0.0.1:8445", Insecure: true},
				CORS:    config.CORSConfig{Credentials: true},
			},
			wantErr: "origins is required when credentials is enabled",
		},
		{
			name: "missing hostPort is rejected",
			cs: config.CodecServer{
				Enabled: true,
				Listen:  config.ListenConfig{Insecure: true},
			},
			wantErr: "hostPort",
		},
		{
			name: "a tls cert without a key is rejected",
			cs: config.CodecServer{
				Enabled: true,
				Listen: config.ListenConfig{
					HostPort: "127.0.0.1:8445",
					TLS:      &config.TLSConfig{Cert: "/nope.pem"},
				},
			},
			wantErr: "certificate and key must be set together",
		},
		{
			name: "a wildcard cors origin is rejected",
			cs: config.CodecServer{
				Enabled: true,
				Listen:  config.ListenConfig{HostPort: "127.0.0.1:8445", Insecure: true},
				CORS:    config.CORSConfig{Origins: []string{"*"}},
			},
			wantErr: `must not contain "*"`,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			err := tc.cs.Validate()
			if tc.wantErr != "" {
				require.ErrorContains(t, err, tc.wantErr)

				return
			}

			require.NoError(t, err)
		})
	}
}
