package testutil_test

import (
	"crypto/ecdsa"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/temporalio/temporal-proxy/pkg/testutil"
)

func TestRSACert(t *testing.T) {
	t.Parallel()

	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(42),
		Subject:      pkix.Name{CommonName: "rsa-test"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
	}

	cert := parseSingleCert(t, testutil.RSACert(t, tmpl))
	require.Equal(t, "rsa-test", cert.Subject.CommonName)
	require.IsType(t, &rsa.PublicKey{}, cert.PublicKey)
	require.Equal(t, 2048, cert.PublicKey.(*rsa.PublicKey).Size()*8)
}

func TestECDSACert(t *testing.T) {
	t.Parallel()

	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(43),
		Subject:      pkix.Name{CommonName: "ecdsa-test"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
	}

	cert := parseSingleCert(t, testutil.ECDSACert(t, tmpl))
	require.Equal(t, "ecdsa-test", cert.Subject.CommonName)

	pub, ok := cert.PublicKey.(*ecdsa.PublicKey)
	require.True(t, ok)
	require.Equal(t, "P-256", pub.Curve.Params().Name)
}

func TestGenerateSelfSignedCert(t *testing.T) {
	t.Parallel()

	certFile, keyFile := testutil.GenerateSelfSignedCert(t)

	// Both files should be loadable as a tls.Certificate pair, which verifies
	// the key matches the cert.
	pair, err := tls.LoadX509KeyPair(certFile, keyFile)
	require.NoError(t, err)
	require.NotEmpty(t, pair.Certificate)

	certPEM, err := os.ReadFile(certFile)
	require.NoError(t, err)

	cert := parseSingleCert(t, certPEM)
	require.Equal(t, "localhost", cert.Subject.CommonName)
	require.Equal(t, []string{"localhost"}, cert.DNSNames)
	require.Contains(t, cert.ExtKeyUsage, x509.ExtKeyUsageServerAuth)

	// Cert window: one hour total, centered roughly on now.
	require.WithinDuration(t, time.Now(), cert.NotBefore, 2*time.Hour)
	require.WithinDuration(t, time.Now(), cert.NotAfter, 2*time.Hour)
}

func TestGenerateMTLSCerts(t *testing.T) {
	t.Parallel()

	caFile, certFile, keyFile := testutil.GenerateMTLSCerts(t)

	// Leaf + key load as a pair.
	_, err := tls.LoadX509KeyPair(certFile, keyFile)
	require.NoError(t, err)

	caPEM, err := os.ReadFile(caFile)
	require.NoError(t, err)
	caCert := parseSingleCert(t, caPEM)
	require.True(t, caCert.IsCA, "CA cert must have IsCA=true")
	require.Equal(t, "test-ca", caCert.Subject.CommonName)

	leafPEM, err := os.ReadFile(certFile)
	require.NoError(t, err)
	leaf := parseSingleCert(t, leafPEM)
	require.Equal(t, "localhost", leaf.Subject.CommonName)

	// The leaf must verify against the CA, otherwise the helper has produced
	// material that can't be used for an mTLS handshake.
	pool := x509.NewCertPool()
	pool.AddCert(caCert)
	_, err = leaf.Verify(x509.VerifyOptions{
		Roots:     pool,
		KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		DNSName:   "localhost",
	})
	require.NoError(t, err)
}

func parseSingleCert(t *testing.T, pemBytes []byte) *x509.Certificate {
	t.Helper()

	block, rest := pem.Decode(pemBytes)
	require.NotNil(t, block, "expected a PEM block")
	require.Equal(t, "CERTIFICATE", block.Type)
	require.Empty(t, rest, "expected exactly one PEM block")

	cert, err := x509.ParseCertificate(block.Bytes)
	require.NoError(t, err)
	return cert
}
