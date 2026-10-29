package auth_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/metadata"

	"go.temporal.io/proxy/internal/api"
	"go.temporal.io/proxy/internal/auth"
	"go.temporal.io/proxy/internal/config"
	"go.temporal.io/proxy/internal/transport/meta"
)

func TestFor(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		cfg     *config.AuthConfig
		conns   api.Connections
		wantErr string
	}{
		{
			name: "no block admits all",
			cfg:  nil,
		},
		{
			name: "static token",
			cfg:  &config.AuthConfig{StaticToken: &config.StaticTokenConfig{Token: "s3cret"}},
		},
		{
			name:    "external names an unknown extension server",
			cfg:     &config.AuthConfig{External: &config.ExternalAuthConfig{Name: "nope"}},
			conns:   api.Connections{},
			wantErr: `names unknown extension server "nope"`,
		},
		{
			name:    "block that sets no method",
			cfg:     &config.AuthConfig{},
			wantErr: "exactly one of external, staticToken, or jwks must be set",
		},
		{
			name: "block that sets two methods",
			cfg: &config.AuthConfig{
				StaticToken: &config.StaticTokenConfig{Token: "s3cret"},
				JWKS:        &config.JWKSConfig{URL: "https://issuer.example/jwks"},
			},
			wantErr: "exactly one of external, staticToken, or jwks must be set",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got, err := auth.For(tc.cfg, tc.conns)
			if tc.wantErr != "" {
				require.ErrorContains(t, err, tc.wantErr)
				require.Nil(t, got)

				return
			}

			require.NoError(t, err)
			require.NotNil(t, got)
		})
	}
}

// TestForPassesNoScheme pins that noScheme in config reaches the authenticator,
// so the bare header value is accepted rather than a "Bearer" prefix required.
func TestForPassesNoScheme(t *testing.T) {
	t.Parallel()

	a, err := auth.For(&config.AuthConfig{StaticToken: &config.StaticTokenConfig{
		Token:    "s3cret",
		Header:   "x-token",
		NoScheme: true,
	}}, nil)
	require.NoError(t, err)
	require.NoError(t, a.Authenticate(t.Context(), meta.Target{}, metadata.Pairs("x-token", "s3cret")))
}
