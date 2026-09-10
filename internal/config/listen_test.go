package config_test

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/temporalio/temporal-proxy/internal/config"
	"github.com/temporalio/temporal-proxy/internal/transport/creds"
	"github.com/temporalio/temporal-proxy/pkg/testutil"
	"github.com/temporalio/temporal-proxy/pkg/validation"
)

func TestListenConfig_Validate(t *testing.T) {
	t.Parallel()

	certFile, keyFile := testutil.GenerateSelfSignedCert(t)

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
			name: "insecure alone is legal",
			cfg:  &config.ListenConfig{HostPort: ":8080", Insecure: true},
		},
		{
			// Asking for plaintext while supplying certificates says two
			// contradictory things, so neither is guessed at.
			name: "insecure with a tls block is contradictory",
			cfg: &config.ListenConfig{
				HostPort: ":8080",
				Insecure: true,
				TLS:      &config.TLSConfig{Cert: certFile, Key: keyFile},
			},
			wantErrs: []validation.Error{
				{Field: "insecure", Message: "cannot be set together with tls"},
			},
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

func TestListenConfig_Dialer(t *testing.T) {
	t.Parallel()

	caFile, certFile, keyFile := testutil.GenerateMTLSCerts(t)

	tests := []struct {
		name string
		cfg  *config.ListenConfig
		want creds.Mode
	}{
		{
			// TLS is the default for an outbound target: with nothing configured
			// the peer is verified against the system roots.
			name: "no tls block verifies against the system roots",
			cfg:  &config.ListenConfig{HostPort: "upstream.example:7233"},
			want: creds.ModeSystemTLS,
		},
		{
			name: "insecure opts out of transport security",
			cfg:  &config.ListenConfig{HostPort: "localhost:7233", Insecure: true},
			want: creds.ModeInsecure,
		},
		{
			name: "a ca pins the peer to a private anchor",
			cfg:  &config.ListenConfig{HostPort: "upstream.example:7233", TLS: &config.TLSConfig{CA: caFile}},
			want: creds.ModeCustomCA,
		},
		{
			name: "a client key pair selects mutual TLS",
			cfg: &config.ListenConfig{
				HostPort: "upstream.example:7233",
				TLS:      &config.TLSConfig{CA: caFile, Cert: certFile, Key: keyFile},
			},
			want: creds.ModeMutualTLS,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			require.Equal(t, tt.want, tt.cfg.Dialer().Mode())
		})
	}
}

func TestListenConfig_Listener(t *testing.T) {
	t.Parallel()

	caFile, certFile, keyFile := testutil.GenerateMTLSCerts(t)

	tests := []struct {
		name string
		cfg  *config.ListenConfig
		want creds.Mode
	}{
		{
			// Unlike a dialer, a listener cannot fall back on the system roots: it
			// has nothing to present, so it stays plaintext.
			name: "no tls block serves plaintext",
			cfg:  &config.ListenConfig{HostPort: ":7233"},
			want: creds.ModeInsecure,
		},
		{
			name: "insecure serves plaintext",
			cfg:  &config.ListenConfig{HostPort: ":7233", Insecure: true},
			want: creds.ModeInsecure,
		},
		{
			name: "a key pair presents a server certificate",
			cfg:  &config.ListenConfig{HostPort: ":7233", TLS: &config.TLSConfig{Cert: certFile, Key: keyFile}},
			want: creds.ModeServerTLS,
		},
		{
			name: "a ca requires a client certificate too",
			cfg: &config.ListenConfig{
				HostPort: ":7233",
				TLS:      &config.TLSConfig{CA: caFile, Cert: certFile, Key: keyFile},
			},
			want: creds.ModeMutualTLS,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			require.Equal(t, tt.want, tt.cfg.Listener().Mode())
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
