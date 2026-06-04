package creds_test

import (
	"crypto/x509"
	"crypto/x509/pkix"
	"math/big"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/temporalio/temporal-proxy/internal/transport/creds"
	"github.com/temporalio/temporal-proxy/pkg/testutil"
)

func TestTLS_DialOption(t *testing.T) {
	t.Parallel()

	// DialOption does not load any files; file paths are irrelevant.
	opt, err := creds.NewClientTLS().DialOption()
	require.NoError(t, err)
	require.NotNil(t, opt)
}

func TestTLS_ServerOption(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name    string
		setup   func(t *testing.T) (certFile, keyFile string)
		wantErr string
	}{
		{
			name: "success",
			setup: func(t *testing.T) (string, string) {
				t.Helper()
				return testutil.GenerateSelfSignedCert(t)
			},
		},
		{
			name: "missing cert file",
			setup: func(t *testing.T) (string, string) {
				t.Helper()
				_, keyFile := testutil.GenerateSelfSignedCert(t)
				return filepath.Join(t.TempDir(), "missing.pem"), keyFile
			},
			wantErr: "failed to load server key pair",
		},
		{
			name: "missing key file",
			setup: func(t *testing.T) (string, string) {
				t.Helper()
				certFile, _ := testutil.GenerateSelfSignedCert(t)
				return certFile, filepath.Join(t.TempDir(), "missing.pem")
			},
			wantErr: "failed to load server key pair",
		},
		{
			name: "invalid cert content",
			setup: func(t *testing.T) (string, string) {
				t.Helper()
				dir := t.TempDir()
				certFile := testutil.WriteFile(t, dir, "cert.pem", []byte("not a cert"))
				keyFile := testutil.WriteFile(t, dir, "key.pem", []byte("not a key"))
				return certFile, keyFile
			},
			wantErr: "failed to load server key pair",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			certFile, keyFile := tc.setup(t)
			opt, err := creds.NewServerTLS(certFile, keyFile).ServerOption()
			if tc.wantErr != "" {
				require.ErrorContains(t, err, tc.wantErr)
				require.Nil(t, opt)
				return
			}

			require.NoError(t, err)
			require.NotNil(t, opt)
		})
	}
}

func TestTLS_Validate(t *testing.T) {
	t.Parallel()

	t.Run("valid RSA cert passes", func(t *testing.T) {
		t.Parallel()

		// preferredCipherSuites are RSA-only, so the leaf must use an RSA key.
		// Validate only checks that the key file exists and is PEM; it does
		// not verify cert/key matching (LoadX509KeyPair does that at runtime).
		certFile := writePEMFile(t, testutil.RSACert(t, validTemplate()))
		require.NoError(t, creds.NewServerTLS(certFile, validKey(t)).Validate())
	})

	t.Run("missing cert file", func(t *testing.T) {
		t.Parallel()

		err := creds.NewServerTLS(filepath.Join(t.TempDir(), "missing.pem"), "").Validate()
		require.ErrorContains(t, err, "failed to read PEM file")
	})

	t.Run("expired cert fails", func(t *testing.T) {
		t.Parallel()

		expired := testutil.RSACert(t, &x509.Certificate{
			SerialNumber: big.NewInt(1),
			Subject:      pkix.Name{CommonName: "expired"},
			NotBefore:    time.Now().Add(-2 * time.Hour),
			NotAfter:     time.Now().Add(-time.Hour),
		})
		certFile := writePEMFile(t, expired)

		err := creds.NewServerTLS(certFile, "").Validate()
		require.ErrorContains(t, err, "expired")
	})

	t.Run("ECDSA cert rejected by RSA-only cipher suites", func(t *testing.T) {
		t.Parallel()

		certFile := writePEMFile(t, testutil.ECDSACert(t, validTemplate()))
		err := creds.NewServerTLS(certFile, "").Validate()
		require.ErrorContains(t, err, "key type")
	})

	t.Run("client TLS has no certFile and fails to read", func(t *testing.T) {
		t.Parallel()

		// NewClientTLS leaves certFile empty; Validate is documented as
		// server-mode-only, but we still want to confirm it surfaces a clear
		// IO error rather than panicking.
		err := creds.NewClientTLS().Validate()
		require.ErrorContains(t, err, "failed to read PEM file")
	})
}
