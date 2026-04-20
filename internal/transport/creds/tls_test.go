package creds_test

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/temporalio/temporal-proxy/internal/transport/creds"
)

func TestTLS_DialOption(t *testing.T) {
	t.Parallel()

	// DialOption does not load any files; file paths are irrelevant.
	opt, err := creds.NewTLS("", "").DialOption()
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
				return mustGenSelfSignedCert(t)
			},
		},
		{
			name: "missing cert file",
			setup: func(t *testing.T) (string, string) {
				t.Helper()
				_, keyFile := mustGenSelfSignedCert(t)
				return filepath.Join(t.TempDir(), "missing.pem"), keyFile
			},
			wantErr: "failed to load server key pair",
		},
		{
			name: "missing key file",
			setup: func(t *testing.T) (string, string) {
				t.Helper()
				certFile, _ := mustGenSelfSignedCert(t)
				return certFile, filepath.Join(t.TempDir(), "missing.pem")
			},
			wantErr: "failed to load server key pair",
		},
		{
			name: "invalid cert content",
			setup: func(t *testing.T) (string, string) {
				t.Helper()
				dir := t.TempDir()
				certFile := writeFile(t, dir, "cert.pem", []byte("not a cert"))
				keyFile := writeFile(t, dir, "key.pem", []byte("not a key"))
				return certFile, keyFile
			},
			wantErr: "failed to load server key pair",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			certFile, keyFile := tc.setup(t)
			opt, err := creds.NewTLS(certFile, keyFile).ServerOption()
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

func mustGenSelfSignedCert(t *testing.T) (certFile, keyFile string) {
	t.Helper()

	dir := t.TempDir()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)

	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "localhost"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		DNSNames:     []string{"localhost"},
	}

	certDER, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	require.NoError(t, err)

	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER})
	certFile = writeFile(t, dir, "cert.pem", certPEM)

	keyDER, err := x509.MarshalECPrivateKey(key)
	require.NoError(t, err)

	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})
	keyFile = writeFile(t, dir, "key.pem", keyPEM)

	return certFile, keyFile
}
