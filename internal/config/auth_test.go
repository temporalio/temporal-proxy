package config_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/temporalio/temporal-proxy/internal/config"
)

func TestAuthConfigValidate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		cfg     config.AuthConfig
		wantErr string // substring; "" means valid
	}{
		{
			name: "valid static token",
			cfg:  config.AuthConfig{StaticToken: &config.StaticTokenConfig{Token: "secret"}},
		},
		{
			name: "valid jwks",
			cfg:  config.AuthConfig{JWKS: &config.JWKSConfig{URL: "https://issuer.example.com/jwks.json"}},
		},
		{
			name:    "none set",
			cfg:     config.AuthConfig{},
			wantErr: "exactly one",
		},
		{
			name: "both set",
			cfg: config.AuthConfig{
				StaticToken: &config.StaticTokenConfig{Token: "secret"},
				JWKS:        &config.JWKSConfig{URL: "https://issuer.example.com/jwks.json"},
			},
			wantErr: "exactly one",
		},
		{
			name:    "static token missing token",
			cfg:     config.AuthConfig{StaticToken: &config.StaticTokenConfig{}},
			wantErr: "token",
		},
		{
			name:    "jwks missing url",
			cfg:     config.AuthConfig{JWKS: &config.JWKSConfig{}},
			wantErr: "url",
		},
		{
			name:    "jwks bad url",
			cfg:     config.AuthConfig{JWKS: &config.JWKSConfig{URL: "not-a-url"}},
			wantErr: "url",
		},
		{
			name:    "jwks http url",
			cfg:     config.AuthConfig{JWKS: &config.JWKSConfig{URL: "http://issuer.example.com/jwks.json"}},
			wantErr: "https",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			err := tt.cfg.Validate()
			if tt.wantErr == "" {
				require.NoError(t, err)
				return
			}
			require.ErrorContains(t, err, tt.wantErr)
		})
	}
}
