package config_test

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/temporalio/temporal-proxy/internal/config"
	"github.com/temporalio/temporal-proxy/pkg/validation"
)

func TestListenConfig_Validate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		cfg      *config.ListenConfig
		wantErrs []validation.Error
	}{
		{
			name: "no TLS, valid hostPort",
			cfg:  &config.ListenConfig{HostPort: ":8080"},
		},
		{
			name: "invalid hostPort",
			cfg:  &config.ListenConfig{HostPort: "localhost"},
			wantErrs: []validation.Error{
				{Field: "hostPort", Message: "is not a valid host:port"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := tt.cfg.Validate()
			if len(tt.wantErrs) == 0 {
				require.NoError(t, err)
				return
			}

			var errs validation.Errors
			require.True(t, errors.As(err, &errs), "expected validation.Errors, got %T", err)
			require.ElementsMatch(t, tt.wantErrs, []validation.Error(errs))
		})
	}
}

func TestTLSConfig_Validate(t *testing.T) {
	t.Parallel()

	// An empty inbound TLS block resolves to server TLS but supplies no
	// certificate, so validation reports a single legality failure. The
	// per-mode certificate-content checks live in the creds package tests.
	err := (&config.TLSConfig{}).Validate()
	require.ErrorContains(t, err, "a server certificate is required")
}
