package auth_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/temporalio/temporal-proxy/internal/api"
	"github.com/temporalio/temporal-proxy/internal/auth"
	"github.com/temporalio/temporal-proxy/internal/config"
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
			name:    "none selected",
			cfg:     &config.AuthConfig{},
			wantErr: "exactly one of external, staticToken, or jwks must be configured",
		},
		{
			name: "several selected",
			cfg: &config.AuthConfig{
				StaticToken: &config.StaticTokenConfig{Token: "s3cret"},
				JWKS:        &config.JWKSConfig{URL: "https://example.test/jwks"},
			},
			wantErr: "exactly one of external, staticToken, or jwks must be configured",
		},
		{
			name:    "external names an unknown extension server",
			cfg:     &config.AuthConfig{External: &config.ExternalAuthConfig{Name: "nope"}},
			conns:   api.Connections{},
			wantErr: `names unknown extension server "nope"`,
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
