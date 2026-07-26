package creds_test

import (
	"crypto/x509"
	"crypto/x509/pkix"
	"math/big"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/temporalio/temporal-proxy/internal/transport/creds"
	"github.com/temporalio/temporal-proxy/pkg/testutil"
)

func TestListener_Mode(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		opts []creds.Option
		want creds.Mode
	}{
		{
			name: "explicit insecure",
			opts: []creds.Option{creds.Insecure()},
			want: creds.ModeInsecure,
		},
		{
			name: "no material requires own certificate (server TLS)",
			opts: nil,
			want: creds.ModeServerTLS,
		},
		{
			name: "certificate only is server TLS",
			opts: []creds.Option{creds.WithCertificate("cert.pem", "key.pem")},
			want: creds.ModeServerTLS,
		},
		{
			name: "CA present requires client certs (mutual TLS)",
			opts: []creds.Option{creds.WithCA("ca.pem"), creds.WithCertificate("cert.pem", "key.pem")},
			want: creds.ModeMutualTLS,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tc.want, creds.NewListener(tc.opts...).Mode())
		})
	}
}

func TestListener_Validate(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name    string
		opts    []creds.Option
		wantErr string
	}{
		{
			name:    "server TLS requires a certificate",
			opts:    nil,
			wantErr: "a server certificate is required",
		},
		{
			name:    "mutual TLS requires the server's own certificate",
			opts:    []creds.Option{creds.WithCA("ca.pem")},
			wantErr: "a server certificate is required",
		},
		{
			name:    "certificate without key",
			opts:    []creds.Option{creds.WithCertificate("cert.pem", "")},
			wantErr: "certificate and key must be set together",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			require.ErrorContains(t, creds.NewListener(tc.opts...).Validate(), tc.wantErr)
		})
	}

	t.Run("insecure", func(t *testing.T) {
		t.Parallel()
		require.NoError(t, creds.NewListener(creds.Insecure()).Validate())
	})
}

func TestListener_Validate_Files(t *testing.T) {
	t.Parallel()

	t.Run("server TLS with valid ECDSA leaf", func(t *testing.T) {
		cert := testutil.ECDSACert(t, &x509.Certificate{
			SerialNumber: big.NewInt(1),
			Subject:      pkix.Name{CommonName: "test"},
			NotBefore:    time.Now().Add(-time.Hour),
			NotAfter:     time.Now().Add(time.Hour),
		})

		_, keyFile := testutil.GenerateSelfSignedCert(t)
		certFile := testutil.WriteFile(t, t.TempDir(), "cert.crt", cert)

		require.NoError(t, creds.NewListener(creds.WithCertificate(certFile, keyFile)).Validate())
	})

	t.Run("server TLS with valid RSA leaf", func(t *testing.T) {
		t.Parallel()
		_, cert, key := testutil.GenerateMTLSCerts(t)
		require.NoError(t, creds.NewListener(creds.WithCertificate(cert, key)).Validate())
	})

	t.Run("mutual TLS with valid material", func(t *testing.T) {
		t.Parallel()
		ca, cert, key := testutil.GenerateMTLSCerts(t)
		require.NoError(t, creds.NewListener(creds.WithCA(ca), creds.WithCertificate(cert, key)).Validate())
	})

	t.Run("missing certificate file surfaces a read error", func(t *testing.T) {
		t.Parallel()
		_, _, key := testutil.GenerateMTLSCerts(t)
		err := creds.NewListener(creds.WithCertificate("/no/such/cert.pem", key)).Validate()
		require.ErrorContains(t, err, "failed to read")
	})
}

func TestListener_ServerOption(t *testing.T) {
	t.Parallel()

	t.Run("insecure", func(t *testing.T) {
		t.Parallel()
		opt, err := creds.NewListener(creds.Insecure()).ServerOption()
		require.NoError(t, err)
		require.NotNil(t, opt)
	})

	t.Run("server TLS", func(t *testing.T) {
		t.Parallel()
		_, cert, key := testutil.GenerateMTLSCerts(t)
		opt, err := creds.NewListener(creds.WithCertificate(cert, key)).ServerOption()
		require.NoError(t, err)
		require.NotNil(t, opt)
	})

	t.Run("mutual TLS", func(t *testing.T) {
		t.Parallel()
		ca, cert, key := testutil.GenerateMTLSCerts(t)
		opt, err := creds.NewListener(creds.WithCA(ca), creds.WithCertificate(cert, key)).ServerOption()
		require.NoError(t, err)
		require.NotNil(t, opt)
	})

	t.Run("missing certificate is illegal", func(t *testing.T) {
		t.Parallel()
		_, err := creds.NewListener().ServerOption()
		require.ErrorContains(t, err, "a server certificate is required")
	})
}

func TestListener_Encrypted(t *testing.T) {
	t.Parallel()

	_, cert, key := testutil.GenerateMTLSCerts(t)

	require.False(t, creds.NewListener(creds.Insecure()).Encrypted())
	require.True(t, creds.NewListener(creds.WithCertificate(cert, key)).Encrypted())
	require.True(t, creds.NewListener(creds.WithCA(cert), creds.WithCertificate(cert, key)).Encrypted())
}
