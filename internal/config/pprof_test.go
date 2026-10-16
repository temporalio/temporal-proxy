package config_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"go.temporal.io/proxy/internal/config"
)

func TestPProfValidate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		p       config.PProf
		wantErr string
	}{
		{
			name: "a disabled block is not checked",
			p: config.PProf{
				Enabled: false,
				Auth:    &config.AuthConfig{},
			},
		},
		{
			name: "an enabled block without auth is valid",
			p:    config.PProf{Enabled: true},
		},
		{
			name: "auth is checked when configured",
			p: config.PProf{
				Enabled: true,
				Auth:    &config.AuthConfig{},
			},
			wantErr: "exactly one",
		},
		{
			name: "a static token passes",
			p: config.PProf{
				Enabled: true,
				Auth: &config.AuthConfig{
					StaticToken: &config.StaticTokenConfig{Token: "s3cret"},
				},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			err := tc.p.Validate()
			if tc.wantErr != "" {
				require.ErrorContains(t, err, tc.wantErr)

				return
			}

			require.NoError(t, err)
		})
	}
}

func TestLoadPProf(t *testing.T) {
	t.Parallel()

	cfg, err := config.Load(strings.NewReader(
		"hostPort: :8080\n" + upstreamYAML + "http:\n  hostPort: 127.0.0.1:8445\n  insecure: true\n" +
			"  pprof:\n    enabled: true\n    auth:\n      staticToken:\n        token: s3cret\n",
	))
	require.NoError(t, err)
	require.True(t, cfg.HTTP.PProf.Enabled)
	require.Equal(t, "s3cret", cfg.HTTP.PProf.Auth.StaticToken.Token)
}
