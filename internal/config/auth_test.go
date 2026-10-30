package config_test

import (
	"testing"

	"github.com/goccy/go-yaml"
	"github.com/stretchr/testify/require"

	"go.temporal.io/proxy/internal/config"
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
			name: "static token noScheme",
			cfg:  config.AuthConfig{StaticToken: &config.StaticTokenConfig{Token: "secret", NoScheme: true}},
		},
		{
			name: "jwks noScheme",
			cfg: config.AuthConfig{JWKS: &config.JWKSConfig{
				URL: "https://issuer.example.com/jwks.json", NoScheme: true,
			}},
		},
		{
			name: "static token noScheme with scheme",
			cfg: config.AuthConfig{StaticToken: &config.StaticTokenConfig{
				Token: "secret", Scheme: "Token", NoScheme: true,
			}},
			wantErr: "noScheme: cannot be set with scheme",
		},
		{
			name: "jwks noScheme with scheme",
			cfg: config.AuthConfig{JWKS: &config.JWKSConfig{
				URL: "https://issuer.example.com/jwks.json", Scheme: "Bearer", NoScheme: true,
			}},
			wantErr: "noScheme: cannot be set with scheme",
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

func TestCredentialConfigValidate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		cfg     config.CredentialConfig
		wantErr string
	}{
		{"valid static", config.CredentialConfig{Static: &config.StaticCredentialConfig{APIKey: "k"}}, ""},
		{"missing static", config.CredentialConfig{}, "static"},
		{"static missing apiKey", config.CredentialConfig{Static: &config.StaticCredentialConfig{}}, "apiKey"},
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

// TestAuthConfigNoSchemeDecoding pins the noScheme YAML key, and that leaving it
// out keeps the historical "Bearer" default.
func TestAuthConfigNoSchemeDecoding(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		yaml string
		want bool
	}{
		{"omitted", "{}", false},
		{"empty scheme", `scheme: ""`, false},
		{"set", "noScheme: true", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var jwks config.JWKSConfig
			require.NoError(t, yaml.Unmarshal([]byte(tt.yaml), &jwks))
			require.Equal(t, tt.want, jwks.NoScheme)

			var static config.StaticTokenConfig
			require.NoError(t, yaml.Unmarshal([]byte(tt.yaml), &static))
			require.Equal(t, tt.want, static.NoScheme)
		})
	}
}
