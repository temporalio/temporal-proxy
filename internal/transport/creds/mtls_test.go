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

func TestMTLS_DialOption(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name    string
		setup   func(t *testing.T) (caFile, certFile, keyFile string)
		wantErr string
	}{
		{
			name: "success",
			setup: func(t *testing.T) (string, string, string) {
				t.Helper()
				return mustGenMTLSCerts(t)
			},
		},
		{
			name: "missing cert file",
			setup: func(t *testing.T) (string, string, string) {
				t.Helper()
				caFile, _, keyFile := mustGenMTLSCerts(t)
				return caFile, filepath.Join(t.TempDir(), "missing.pem"), keyFile
			},
			wantErr: "failed to load client key pair",
		},
		{
			name: "missing key file",
			setup: func(t *testing.T) (string, string, string) {
				t.Helper()
				caFile, certFile, _ := mustGenMTLSCerts(t)
				return caFile, certFile, filepath.Join(t.TempDir(), "missing.pem")
			},
			wantErr: "failed to load client key pair",
		},
		{
			name: "missing CA file",
			setup: func(t *testing.T) (string, string, string) {
				t.Helper()
				_, certFile, keyFile := mustGenMTLSCerts(t)
				return filepath.Join(t.TempDir(), "missing.pem"), certFile, keyFile
			},
			wantErr: "failed to load CA certificate",
		},
		{
			name: "invalid CA PEM",
			setup: func(t *testing.T) (string, string, string) {
				t.Helper()
				dir := t.TempDir()
				_, certFile, keyFile := mustGenMTLSCerts(t)
				caFile := writeFile(t, dir, "ca.pem", []byte("not pem"))
				return caFile, certFile, keyFile
			},
			wantErr: "failed to parse CA file",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			caFile, certFile, keyFile := tc.setup(t)
			opt, err := creds.NewMTLS(caFile, certFile, keyFile, creds.MTLSOptions{}).DialOption()
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

func TestMTLS_Options(t *testing.T) {
	t.Parallel()

	t.Run("InsecureSkipVerify is propagated", func(t *testing.T) {
		t.Parallel()

		caFile, certFile, keyFile := mustGenMTLSCerts(t)
		opt, err := creds.NewMTLS(caFile, certFile, keyFile, creds.MTLSOptions{
			InsecureSkipVerify: true,
		}).DialOption()
		require.NoError(t, err)
		require.NotNil(t, opt)
	})

	t.Run("ServerName is propagated", func(t *testing.T) {
		t.Parallel()

		caFile, certFile, keyFile := mustGenMTLSCerts(t)
		opt, err := creds.NewMTLS(caFile, certFile, keyFile, creds.MTLSOptions{
			ServerName: "example.com",
		}).DialOption()
		require.NoError(t, err)
		require.NotNil(t, opt)
	})
}

func TestMTLS_ServerOption(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name    string
		setup   func(t *testing.T) (caFile, certFile, keyFile string)
		wantErr string
	}{
		{
			name: "success",
			setup: func(t *testing.T) (string, string, string) {
				t.Helper()
				return mustGenMTLSCerts(t)
			},
		},
		{
			name: "missing cert file",
			setup: func(t *testing.T) (string, string, string) {
				t.Helper()
				caFile, _, keyFile := mustGenMTLSCerts(t)
				return caFile, filepath.Join(t.TempDir(), "missing.pem"), keyFile
			},
			wantErr: "failed to load server key pair",
		},
		{
			name: "missing key file",
			setup: func(t *testing.T) (string, string, string) {
				t.Helper()
				caFile, certFile, _ := mustGenMTLSCerts(t)
				return caFile, certFile, filepath.Join(t.TempDir(), "missing.pem")
			},
			wantErr: "failed to load server key pair",
		},
		{
			name: "missing CA file",
			setup: func(t *testing.T) (string, string, string) {
				t.Helper()
				_, certFile, keyFile := mustGenMTLSCerts(t)
				return filepath.Join(t.TempDir(), "missing.pem"), certFile, keyFile
			},
			wantErr: "failed to load CA certificate",
		},
		{
			name: "invalid CA PEM",
			setup: func(t *testing.T) (string, string, string) {
				t.Helper()
				dir := t.TempDir()
				_, certFile, keyFile := mustGenMTLSCerts(t)
				caFile := writeFile(t, dir, "ca.pem", []byte("not pem"))
				return caFile, certFile, keyFile
			},
			wantErr: "failed to parse CA file",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			caFile, certFile, keyFile := tc.setup(t)
			opt, err := creds.NewMTLS(caFile, certFile, keyFile, creds.MTLSOptions{}).ServerOption()
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

func mustGenMTLSCerts(t *testing.T) (caFile, certFile, keyFile string) {
	t.Helper()

	dir := t.TempDir()

	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)

	caTmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(2),
		Subject:               pkix.Name{CommonName: "test-ca"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}

	caDER, err := x509.CreateCertificate(rand.Reader, caTmpl, caTmpl, &caKey.PublicKey, caKey)
	require.NoError(t, err)

	caCert, err := x509.ParseCertificate(caDER)
	require.NoError(t, err)

	caPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER})
	caFile = writeFile(t, dir, "ca.pem", caPEM)

	leafKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)

	leafTmpl := &x509.Certificate{
		SerialNumber: big.NewInt(3),
		Subject:      pkix.Name{CommonName: "localhost"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		DNSNames:     []string{"localhost"},
	}

	leafDER, err := x509.CreateCertificate(rand.Reader, leafTmpl, caCert, &leafKey.PublicKey, caKey)
	require.NoError(t, err)

	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: leafDER})
	certFile = writeFile(t, dir, "cert.pem", certPEM)

	leafKeyDER, err := x509.MarshalECPrivateKey(leafKey)
	require.NoError(t, err)

	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: leafKeyDER})
	keyFile = writeFile(t, dir, "key.pem", keyPEM)

	return caFile, certFile, keyFile
}
