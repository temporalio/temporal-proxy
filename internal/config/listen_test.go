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

// TestListenConfig_Validate_TLS_RouteSelection exercises the branch in
// TLSConfig.Validate that picks creds.TLS when CA is empty and creds.MTLS
// when CA is set. The deep cert-content checks live in creds' own tests; we
// fingerprint the routing decision via the (Subject, Field) tuples that
// surface for missing files, which is stable across both routes.
func TestListenConfig_Validate_TLS_RouteSelection(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		tls  *config.TLSConfig
		// Each entry is the (Subject, Field) tuple expected on one Error.
		// Messages are intentionally not pinned: creds errors include the
		// host's filesystem-error wording which differs across OSes.
		wantTuples [][2]string
	}{
		{
			name: "empty TLS routes through creds.TLS (CA empty)",
			tls:  &config.TLSConfig{},
			wantTuples: [][2]string{
				{"tls", "cert"}, // creds.TLS PEM read failure on cert
				{"tls", "key"},  // creds.TLS PEM read failure on key
			},
		},
		{
			name: "missing-file paths route through creds.TLS (CA empty)",
			tls: &config.TLSConfig{
				Cert: "/missing/cert.pem",
				Key:  "/missing/key.pem",
			},
			wantTuples: [][2]string{
				{"tls", "cert"}, // creds.TLS PEM read failure on cert
				{"tls", "key"},  // creds.TLS PEM read failure on key (no "ca")
			},
		},
		{
			name: "CA set routes through creds.MTLS",
			tls: &config.TLSConfig{
				Cert: "/missing/cert.pem",
				Key:  "/missing/key.pem",
				CA:   "/missing/ca.pem",
			},
			wantTuples: [][2]string{
				{"tls", "cert"}, // creds.MTLS PEM read failure on cert
				{"tls", "key"},  // creds.MTLS PEM read failure on key
				{"tls", "ca"},   // creds.MTLS PEM read failure on ca
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cfg := &config.ListenConfig{HostPort: ":8080", TLS: tt.tls}
			err := cfg.Validate()
			require.Error(t, err)

			var errs validation.Errors
			require.True(t, errors.As(err, &errs))

			got := make([][2]string, len(errs))
			for i, e := range errs {
				got[i] = [2]string{e.Subject, e.Field}
			}
			require.ElementsMatch(t, tt.wantTuples, got)
		})
	}
}

func TestTLSConfig_Validate(t *testing.T) {
	t.Parallel()

	// Called standalone (not via Nested), entries keep the empty Subject
	// that TLSConfig.Validate sets. An empty struct routes through the
	// CA=="" branch (creds.TLS), so we expect cert + key PEM read failures.
	tls := &config.TLSConfig{}
	err := tls.Validate()
	require.Error(t, err)

	var errs validation.Errors
	require.True(t, errors.As(err, &errs))
	require.Len(t, errs, 2)

	for _, e := range errs {
		require.Empty(t, e.Subject, "standalone Validate leaves Subject empty")
	}

	fields := make([]string, len(errs))
	for i, e := range errs {
		fields[i] = e.Field
	}
	require.ElementsMatch(t, []string{"cert", "key"}, fields)
}
