package creds_test

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
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"

	"go.temporal.io/proxy/internal/transport/creds"
	"go.temporal.io/proxy/pkg/testutil"
)

func TestMaterialReload_UnchangedIsNotAnAttempt(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	certFile, keyFile := writeLeaf(t, dir, time.Now().Add(-time.Hour), time.Now().Add(time.Hour))
	caFile := writeCA(t, dir, time.Now().Add(time.Hour))
	d := loadedMutualDialer(t, certFile, keyFile, caFile)

	for range 2 {
		attempted, err := creds.DialerReload(d)
		require.NoError(t, err)
		require.False(t, attempted)
	}
}

func TestMaterialReload_ChangedSwaps(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	certFile, keyFile := writeLeaf(t, dir, time.Now().Add(-time.Hour), time.Now().Add(time.Hour))
	caFile := writeCA(t, dir, time.Now().Add(time.Hour))
	d := loadedMutualDialer(t, certFile, keyFile, caFile)

	notAfter := time.Now().Add(2 * time.Hour).Truncate(time.Second)
	writeLeafTo(t, certFile, keyFile, time.Now().Add(-time.Hour), notAfter)

	attempted, err := creds.DialerReload(d)
	require.NoError(t, err)
	require.True(t, attempted)

	got, _ := creds.DialerSnapshot(d)
	require.True(t, notAfter.Equal(got), "got %s want %s", got, notAfter)
}

func TestMaterialReload_HalfRotatedKeepsOld(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	oldAfter := time.Now().Add(time.Hour)
	certFile, keyFile := writeLeaf(t, dir, time.Now().Add(-time.Hour), oldAfter)
	caFile := writeCA(t, dir, time.Now().Add(time.Hour))
	d := loadedMutualDialer(t, certFile, keyFile, caFile)
	before, _ := creds.DialerSnapshot(d)

	// Write the new pair into a scratch dir, then copy only the cert over.
	newAfter := time.Now().Add(2 * time.Hour).Truncate(time.Second)
	newCert, newKey := writeLeaf(t, t.TempDir(), time.Now().Add(-time.Hour), newAfter)
	copyFile(t, newCert, certFile)

	attempted, err := creds.DialerReload(d)
	require.True(t, attempted)
	require.Error(t, err)

	got, _ := creds.DialerSnapshot(d)
	require.True(t, before.Equal(got))

	copyFile(t, newKey, keyFile)

	attempted, err = creds.DialerReload(d)
	require.True(t, attempted)
	require.NoError(t, err)

	got, _ = creds.DialerSnapshot(d)
	require.True(t, newAfter.Equal(got))
}

func TestMaterialReload_FailedRetriesOnSameBytes(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		dir := t.TempDir()
		now := time.Now()
		certFile, keyFile := writeLeaf(t, dir, now.Add(-time.Hour), now.Add(time.Hour))
		caFile := writeCA(t, dir, now.Add(time.Hour))
		d := loadedMutualDialer(t, certFile, keyFile, caFile)

		newAfter := now.Add(3 * time.Hour).Truncate(time.Second)
		writeLeafTo(t, certFile, keyFile, now.Add(30*time.Second), newAfter)

		attempted, err := creds.DialerReload(d)
		require.True(t, attempted)
		require.ErrorContains(t, err, "not yet valid")

		time.Sleep(time.Minute)

		attempted, err = creds.DialerReload(d)
		require.True(t, attempted)
		require.NoError(t, err)

		got, _ := creds.DialerSnapshot(d)
		require.True(t, newAfter.Equal(got))
	})
}

func TestMaterialReload_MissingFileFails(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	certFile, keyFile := writeLeaf(t, dir, time.Now().Add(-time.Hour), time.Now().Add(time.Hour))
	caFile := writeCA(t, dir, time.Now().Add(time.Hour))
	d := loadedMutualDialer(t, certFile, keyFile, caFile)
	before, _ := creds.DialerSnapshot(d)

	require.NoError(t, os.Remove(keyFile))

	for range 2 {
		attempted, err := creds.DialerReload(d)
		require.True(t, attempted)
		require.Error(t, err)

		got, _ := creds.DialerSnapshot(d)
		require.True(t, before.Equal(got))
	}
}

func TestMaterialReload_SymlinkSwap(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	caFile := writeCA(t, dir, time.Now().Add(time.Hour))

	v1, v2 := filepath.Join(dir, "v1"), filepath.Join(dir, "v2")
	require.NoError(t, os.Mkdir(v1, 0o700))
	require.NoError(t, os.Mkdir(v2, 0o700))

	writeLeaf(t, v1, time.Now().Add(-time.Hour), time.Now().Add(time.Hour))
	newAfter := time.Now().Add(2 * time.Hour).Truncate(time.Second)
	writeLeaf(t, v2, time.Now().Add(-time.Hour), newAfter)

	current := filepath.Join(dir, "current")
	require.NoError(t, os.Symlink(v1, current))

	d := loadedMutualDialer(t, filepath.Join(current, "cert.pem"), filepath.Join(current, "key.pem"), caFile)

	tmp := filepath.Join(dir, "current.tmp")
	require.NoError(t, os.Symlink(v2, tmp))
	require.NoError(t, os.Rename(tmp, current))

	attempted, err := creds.DialerReload(d)
	require.True(t, attempted)
	require.NoError(t, err)

	got, _ := creds.DialerSnapshot(d)
	require.True(t, newAfter.Equal(got))
}

func TestMaterialReload_MutualTLSSwapsAsUnit(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	certFile, keyFile := writeLeaf(t, dir, time.Now().Add(-time.Hour), time.Now().Add(time.Hour))
	caFile := writeCA(t, dir, time.Now().Add(time.Hour))
	d := loadedMutualDialer(t, certFile, keyFile, caFile)
	certBefore, caBefore := creds.DialerSnapshot(d)

	writeLeafTo(t, certFile, keyFile, time.Now().Add(-time.Hour), time.Now().Add(5*time.Hour))
	require.NoError(t, os.WriteFile(caFile, []byte("not a ca"), 0o600))

	attempted, err := creds.DialerReload(d)
	require.True(t, attempted)
	require.Error(t, err)

	certAfter, caAfter := creds.DialerSnapshot(d)
	require.True(t, certBefore.Equal(certAfter))
	require.True(t, caBefore.Equal(caAfter))
}

func TestMaterialReload_CABundleEarliestNotAfter(t *testing.T) {
	t.Parallel()

	now := time.Now().Truncate(time.Second)
	caFile := writeCA(t, t.TempDir(), now.Add(3*time.Hour), now.Add(time.Hour), now.Add(2*time.Hour))

	d := creds.NewDialer(creds.WithCA(caFile))
	_, err := d.DialOption("localhost")
	require.NoError(t, err)

	certAfter, caAfter := creds.DialerSnapshot(d)
	require.True(t, certAfter.IsZero())
	require.True(t, now.Add(time.Hour).Equal(caAfter), "got %s", caAfter)
}

func loadedMutualDialer(t *testing.T, certFile, keyFile, caFile string) *creds.Dialer {
	t.Helper()

	d := creds.NewDialer(creds.WithCertificate(certFile, keyFile), creds.WithCA(caFile))
	_, err := d.DialOption("localhost")
	require.NoError(t, err)

	return d
}

// writeLeaf writes a self-signed ECDSA P-256 leaf (not a CA) and its key into
// dir as cert.pem and key.pem.
func writeLeaf(t *testing.T, dir string, notBefore, notAfter time.Time) (certFile, keyFile string) {
	t.Helper()

	certFile, keyFile = filepath.Join(dir, "cert.pem"), filepath.Join(dir, "key.pem")
	writeLeafTo(t, certFile, keyFile, notBefore, notAfter)

	return certFile, keyFile
}

func writeLeafTo(t *testing.T, certFile, keyFile string, notBefore, notAfter time.Time) {
	t.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)

	serial, err := rand.Int(rand.Reader, big.NewInt(1<<62))
	require.NoError(t, err)

	tmpl := &x509.Certificate{
		SerialNumber: serial,
		Subject:      pkix.Name{CommonName: "localhost"},
		NotBefore:    notBefore,
		NotAfter:     notAfter,
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		DNSNames:     []string{"localhost"},
	}

	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	require.NoError(t, err)

	keyDER, err := x509.MarshalECPrivateKey(key)
	require.NoError(t, err)

	writePEM(t, certFile, "CERTIFICATE", der)
	writePEM(t, keyFile, "EC PRIVATE KEY", keyDER)
}

// writeCA writes one self-signed CA certificate per notAfter, concatenated,
// into ca.pem under dir.
func writeCA(t *testing.T, dir string, notAfters ...time.Time) (caFile string) {
	t.Helper()

	var bundle []byte

	for _, notAfter := range notAfters {
		key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		require.NoError(t, err)

		serial, err := rand.Int(rand.Reader, big.NewInt(1<<62))
		require.NoError(t, err)

		tmpl := &x509.Certificate{
			SerialNumber:          serial,
			Subject:               pkix.Name{CommonName: "test-ca"},
			NotBefore:             time.Now().Add(-time.Hour),
			NotAfter:              notAfter,
			KeyUsage:              x509.KeyUsageCertSign,
			BasicConstraintsValid: true,
			IsCA:                  true,
		}

		der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
		require.NoError(t, err)

		bundle = append(bundle, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})...)
	}

	return testutil.WriteFile(t, dir, "ca.pem", bundle)
}

func writePEM(t *testing.T, path, typ string, der []byte) {
	t.Helper()
	require.NoError(t, os.WriteFile(path, pem.EncodeToMemory(&pem.Block{Type: typ, Bytes: der}), 0o600))
}

func copyFile(t *testing.T, from, to string) {
	t.Helper()

	data, err := os.ReadFile(from)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(to, data, 0o600))
}
