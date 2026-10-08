package config_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/temporalio/temporal-proxy/internal/config"
)

func TestCodecServerValidate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		cs      config.CodecServer
		wantErr string
	}{
		{
			name: "disabled is always valid",
			cs:   config.CodecServer{CORS: config.CORSConfig{Origins: []string{"*"}}},
		},
		{
			name: "enabled with no cors is valid",
			cs:   config.CodecServer{Enabled: true},
		},
		{
			name: "credentials without origins is rejected",
			cs: config.CodecServer{
				Enabled: true,
				CORS:    config.CORSConfig{Credentials: true},
			},
			wantErr: "origins is required when credentials is enabled",
		},
		{
			name: "a wildcard cors origin is rejected",
			cs: config.CodecServer{
				Enabled: true,
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
