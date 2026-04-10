package server_test

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/temporalio/temporal-proxy/internal/server"
)

func TestInsecureCredentialsServer(t *testing.T) {
	t.Parallel()

	opt, err := server.NewInsecureCredentials().Server()
	require.NoError(t, err)
	require.NotNil(t, opt)
}

func TestTLSCredentialsServer(t *testing.T) {
	t.Parallel()

	t.Run("error cases", func(t *testing.T) {
		t.Parallel()

		cases := []struct {
			name        string
			setup       func(t *testing.T) (certFile, keyFile string)
			errContains string
		}{
			{
				name: "missing certificate file",
				setup: func(t *testing.T) (string, string) {
					dir := t.TempDir()
					_, keyPEM := mustCreateSelfSignedServerCertificate(t)
					keyFile := writeFile(t, dir, "server-key.pem", keyPEM)

					return filepath.Join(dir, "missing-cert.pem"), keyFile
				},
				errContains: "failed to create TLS credentials",
			},
			{
				name: "missing key file",
				setup: func(t *testing.T) (string, string) {
					dir := t.TempDir()
					certPEM, _ := mustCreateSelfSignedServerCertificate(t)
					certFile := writeFile(t, dir, "server-cert.pem", certPEM)

					return certFile, filepath.Join(dir, "missing-key.pem")
				},
				errContains: "failed to create TLS credentials",
			},
		}

		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				t.Parallel()

				certFile, keyFile := tc.setup(t)
				opt, err := server.NewTLSCredentials(certFile, keyFile).Server()
				require.Error(t, err)
				require.Nil(t, opt)
				require.ErrorContains(t, err, tc.errContains)
			})
		}
	})

	t.Run("success", func(t *testing.T) {
		t.Parallel()

		dir := t.TempDir()
		certPEM, keyPEM := mustCreateSelfSignedServerCertificate(t)

		certFile := writeFile(t, dir, "server-cert.pem", certPEM)
		keyFile := writeFile(t, dir, "server-key.pem", keyPEM)

		opt, err := server.NewTLSCredentials(certFile, keyFile).Server()
		require.NoError(t, err)
		require.NotNil(t, opt)
	})
}

func TestMTLSCredentialsServer(t *testing.T) {
	t.Parallel()

	t.Run("error cases", func(t *testing.T) {
		t.Parallel()

		cases := []struct {
			name        string
			setup       func(t *testing.T) (caFile, certFile, keyFile string)
			errContains string
		}{
			{
				name: "missing key pair file",
				setup: func(t *testing.T) (string, string, string) {
					dir := t.TempDir()
					return filepath.Join(dir, "ca.pem"), filepath.Join(dir, "cert.pem"), filepath.Join(dir, "key.pem")
				},
				errContains: "failed to load server key pair",
			},
			{
				name: "missing CA file",
				setup: func(t *testing.T) (string, string, string) {
					dir := t.TempDir()
					certPEM, keyPEM := mustCreateSelfSignedServerCertificate(t)
					certFile := writeFile(t, dir, "cert.pem", certPEM)
					keyFile := writeFile(t, dir, "key.pem", keyPEM)

					return filepath.Join(dir, "missing-ca.pem"), certFile, keyFile
				},
				errContains: "failed to load CA certificate",
			},
			{
				name: "invalid CA PEM",
				setup: func(t *testing.T) (string, string, string) {
					dir := t.TempDir()
					certPEM, keyPEM := mustCreateSelfSignedServerCertificate(t)
					certFile := writeFile(t, dir, "cert.pem", certPEM)
					keyFile := writeFile(t, dir, "key.pem", keyPEM)
					caFile := writeFile(t, dir, "ca.pem", []byte("not pem"))

					return caFile, certFile, keyFile
				},
				errContains: "failed to parse CA file",
			},
		}

		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				t.Parallel()

				caFile, certFile, keyFile := tc.setup(t)

				opt, err := server.NewMTLSCredentials(caFile, certFile, keyFile).Server()
				require.Error(t, err)
				require.Nil(t, opt)
				require.ErrorContains(t, err, tc.errContains)
			})
		}
	})

	t.Run("success", func(t *testing.T) {
		t.Parallel()

		dir := t.TempDir()
		caPEM, certPEM, keyPEM := mustCreateMutualTLSServerCertificate(t)
		caFile := writeFile(t, dir, "ca.pem", caPEM)
		certFile := writeFile(t, dir, "cert.pem", certPEM)
		keyFile := writeFile(t, dir, "key.pem", keyPEM)

		opt, err := server.NewMTLSCredentials(caFile, certFile, keyFile).Server()
		require.NoError(t, err)
		require.NotNil(t, opt)
	})
}

func writeFile(t *testing.T, dir, name string, contents []byte) string {
	t.Helper()

	path := filepath.Join(dir, name)
	err := os.WriteFile(path, contents, 0o600)
	require.NoError(t, err)

	return path
}

func mustCreateSelfSignedServerCertificate(t *testing.T) ([]byte, []byte) {
	t.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)

	template := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject: pkix.Name{
			CommonName: "localhost",
		},
		NotBefore:   time.Now().Add(-time.Hour),
		NotAfter:    time.Now().Add(time.Hour),
		KeyUsage:    x509.KeyUsageDigitalSignature,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		DNSNames:    []string{"localhost"},
	}

	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	require.NoError(t, err)

	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})

	keyDER, err := x509.MarshalECPrivateKey(key)
	require.NoError(t, err)
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})

	return certPEM, keyPEM
}

func mustCreateMutualTLSServerCertificate(t *testing.T) ([]byte, []byte, []byte) {
	t.Helper()

	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)

	caTemplate := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject: pkix.Name{
			CommonName: "test-ca",
		},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}

	caDER, err := x509.CreateCertificate(rand.Reader, caTemplate, caTemplate, &caKey.PublicKey, caKey)
	require.NoError(t, err)

	serverKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)

	serverTemplate := &x509.Certificate{
		SerialNumber: big.NewInt(3),
		Subject: pkix.Name{
			CommonName: "localhost",
		},
		NotBefore:   time.Now().Add(-time.Hour),
		NotAfter:    time.Now().Add(time.Hour),
		KeyUsage:    x509.KeyUsageDigitalSignature,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		DNSNames:    []string{"localhost"},
	}

	serverDER, err := x509.CreateCertificate(rand.Reader, serverTemplate, caTemplate, &serverKey.PublicKey, caKey)
	require.NoError(t, err)

	caPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER})
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: serverDER})

	keyDER, err := x509.MarshalECPrivateKey(serverKey)
	require.NoError(t, err)
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})

	return caPEM, certPEM, keyPEM
}
