package certs_test

import (
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/elliptic"
	"crypto/rand"
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
	"github.com/temporalio/temporal-proxy/pkg/validation"
	"github.com/temporalio/temporal-proxy/pkg/validation/certs"
)

func TestValidatePEM_NoChecks(t *testing.T) {
	t.Parallel()

	// Invalid PEM: no checks means the function should return nil without even
	// attempting to parse the data.
	require.NoError(t, certs.ValidatePEM([]byte("not-pem-at-all")))
}

func TestValidatePEMFile(t *testing.T) {
	t.Parallel()

	t.Run("missing file", func(t *testing.T) {
		t.Parallel()

		err := certs.ValidatePEMFile(
			"/tmp/definitely-not-a-real-cert.pem",
			certs.NotExpired(),
		)
		require.Error(t, err)
		require.ErrorContains(t, err, "failed to read PEM file")
	})

	t.Run("missing file with no checks still errors", func(t *testing.T) {
		t.Parallel()

		// The IO read happens before the no-checks short-circuit, so a missing
		// file should surface even without anything to validate.
		err := certs.ValidatePEMFile("/tmp/definitely-not-a-real-cert.pem")
		require.Error(t, err)
		require.ErrorContains(t, err, "failed to read PEM file")
	})

	t.Run("valid cert passes", func(t *testing.T) {
		t.Parallel()

		path := writePEMFile(t, testutil.RSACert(t, validTemplate()))
		require.NoError(t, certs.ValidatePEMFile(
			path,
			certs.NotExpired(),
		))
	})

	t.Run("check failure is propagated", func(t *testing.T) {
		t.Parallel()

		expired := testutil.RSACert(t, &x509.Certificate{
			SerialNumber: big.NewInt(99),
			Subject:      pkix.Name{CommonName: "expired"},
			NotBefore:    time.Now().Add(-2 * time.Hour),
			NotAfter:     time.Now().Add(-time.Hour),
		})
		path := writePEMFile(t, expired)

		err := certs.ValidatePEMFile(path, certs.NotExpired())
		require.Error(t, err)
		require.ErrorContains(t, err, "expired")
	})

	t.Run("non-PEM contents are rejected when checks run", func(t *testing.T) {
		t.Parallel()

		path := writePEMFile(t, []byte("this is not a PEM block"))
		err := certs.ValidatePEMFile(path, certs.NotExpired())
		require.Error(t, err)
		require.ErrorContains(t, err, "no certificates found")
	})
}

func TestValidatePEM_InvalidPEM(t *testing.T) {
	t.Parallel()

	require.Error(t, certs.ValidatePEM([]byte("not-pem"), certs.NotExpired()))
}

func TestValidatePEM_MultipleCerts(t *testing.T) {
	t.Parallel()

	valid := testutil.RSACert(t, validTemplate())
	expired := testutil.RSACert(t, &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      pkix.Name{CommonName: "expired"},
		NotBefore:    time.Now().Add(-2 * time.Hour),
		NotAfter:     time.Now().Add(-time.Hour),
	})

	combined := append(valid, expired...)
	err := certs.ValidatePEM(combined, certs.NotExpired())
	require.Error(t, err)

	var verr validation.Errors
	require.ErrorAs(t, err, &verr)
	require.Len(t, verr, 1)
	require.Equal(t, "expired", verr[0].Subject)
}

func TestNotExpired(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		tmpl    *x509.Certificate
		wantErr bool
		errMsg  string
	}{
		{
			name: "valid cert passes",
			tmpl: &x509.Certificate{
				SerialNumber: big.NewInt(1),
				NotBefore:    time.Now().Add(-time.Hour),
				NotAfter:     time.Now().Add(time.Hour),
			},
			wantErr: false,
		},
		{
			name: "expired cert fails",
			tmpl: &x509.Certificate{
				SerialNumber: big.NewInt(1),
				Subject:      pkix.Name{CommonName: "old"},
				NotBefore:    time.Now().Add(-2 * time.Hour),
				NotAfter:     time.Now().Add(-time.Hour),
			},
			wantErr: true,
			errMsg:  "expired",
		},
		{
			name: "not yet valid cert fails",
			tmpl: &x509.Certificate{
				SerialNumber: big.NewInt(1),
				Subject:      pkix.Name{CommonName: "future"},
				NotBefore:    time.Now().Add(time.Hour),
				NotAfter:     time.Now().Add(2 * time.Hour),
			},
			wantErr: true,
			errMsg:  "not yet valid",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			certPEM := testutil.RSACert(t, tt.tmpl)
			err := certs.ValidatePEM(certPEM, certs.NotExpired())
			if tt.wantErr {
				require.Error(t, err)
				require.Contains(t, err.Error(), tt.errMsg)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestIsCA(t *testing.T) {
	t.Parallel()

	t.Run("CA cert passes", func(t *testing.T) {
		t.Parallel()

		certPEM := testutil.RSACert(t, caTemplate())
		require.NoError(t, certs.ValidatePEM(certPEM, certs.IsCA()))
	})

	t.Run("non-CA cert fails", func(t *testing.T) {
		t.Parallel()

		certPEM := testutil.RSACert(t, validTemplate())
		err := certs.ValidatePEM(certPEM, certs.IsCA())
		require.Error(t, err)
		require.Contains(t, err.Error(), "not a CA")
	})
}

func TestSecureAlgorithm(t *testing.T) {
	t.Parallel()

	rsaSuites := []uint16{
		tls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256, // ECDSA suite
		tls.TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA256,   // RSA suite
	}

	t.Run("RSA cert with no suites passes", func(t *testing.T) {
		t.Parallel()

		certPEM := testutil.RSACert(t, validTemplate())
		require.NoError(t, certs.ValidatePEM(certPEM, certs.SecureAlgorithm()))
	})

	t.Run("ECDSA cert with no suites passes", func(t *testing.T) {
		t.Parallel()

		certPEM := testutil.ECDSACert(t, validTemplate())
		require.NoError(t, certs.ValidatePEM(certPEM, certs.SecureAlgorithm()))
	})

	t.Run("RSA cert with RSA suite passes", func(t *testing.T) {
		t.Parallel()

		certPEM := testutil.RSACert(t, validTemplate())
		require.NoError(t, certs.ValidatePEM(certPEM, certs.SecureAlgorithm(rsaSuites...)))
	})

	t.Run("ECDSA cert incompatible with RSA-only suites fails", func(t *testing.T) {
		t.Parallel()

		rsaOnlySuites := []uint16{
			tls.TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA256,
			tls.TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,
		}

		certPEM := testutil.ECDSACert(t, validTemplate())
		err := certs.ValidatePEM(certPEM, certs.SecureAlgorithm(rsaOnlySuites...))
		require.Error(t, err)
		require.Contains(t, err.Error(), "key type")
	})

	t.Run("unknown suites skip key type check", func(t *testing.T) {
		t.Parallel()

		// 0xFFFF is intentionally not a known suite, so no key type constraint applies.
		certPEM := testutil.ECDSACert(t, validTemplate())
		require.NoError(t, certs.ValidatePEM(certPEM, certs.SecureAlgorithm(0xFFFF)))
	})
}

func TestSecureAlgorithm_WeakAlgorithm(t *testing.T) {
	t.Parallel()

	// Go rejects SHA-1 signing by default, so we build a well-formed modern cert
	// and patch its SignatureAlgorithm to a weak value after parsing to exercise
	// the rejection path without relying on Go producing a SHA-1 signature.
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)

	tmpl := validTemplate()
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	require.NoError(t, err)

	cert, err := x509.ParseCertificate(der)
	require.NoError(t, err)

	cert.SignatureAlgorithm = x509.SHA1WithRSA

	err = certs.SecureAlgorithm()(cert)
	require.Error(t, err)
	require.Contains(t, err.Error(), "weak signature algorithm")
}

func TestSufficientKeySize(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		certPEM func(t *testing.T) []byte
		wantErr bool
		errMsg  string
	}{
		{
			name:    "RSA 2048 passes",
			certPEM: func(t *testing.T) []byte { return rsaCert(t, 2048) },
			wantErr: false,
		},
		{
			name:    "RSA 4096 passes",
			certPEM: func(t *testing.T) []byte { return rsaCert(t, 4096) },
			wantErr: false,
		},
		{
			name:    "RSA 1024 fails",
			certPEM: func(t *testing.T) []byte { return rsaCert(t, 1024) },
			wantErr: true,
			errMsg:  "RSA key size 1024 below minimum 2048",
		},
		{
			name:    "ECDSA P-256 passes",
			certPEM: func(t *testing.T) []byte { return ecdsaCert(t, elliptic.P256()) },
			wantErr: false,
		},
		{
			name:    "ECDSA P-384 passes",
			certPEM: func(t *testing.T) []byte { return ecdsaCert(t, elliptic.P384()) },
			wantErr: false,
		},
		{
			name:    "ECDSA P-224 fails",
			certPEM: func(t *testing.T) []byte { return ecdsaCert(t, elliptic.P224()) },
			wantErr: true,
			errMsg:  "ECDSA key size 224 below minimum 256",
		},
		{
			name:    "Ed25519 fails",
			certPEM: ed25519Cert,
			wantErr: true,
			errMsg:  "unsupported public key type",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := certs.ValidatePEM(tt.certPEM(t), certs.SufficientKeySize())
			if tt.wantErr {
				require.Error(t, err)
				require.Contains(t, err.Error(), tt.errMsg)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestValidatePEMKeyFile(t *testing.T) {
	t.Parallel()

	t.Run("valid private key file", func(t *testing.T) {
		t.Parallel()

		require.NoError(t, certs.ValidatePEMKeyFile(validKey(t)))
	})

	t.Run("missing file", func(t *testing.T) {
		t.Parallel()

		err := certs.ValidatePEMKeyFile("/tmp/definitely-not-a-real-key.pem")
		require.Error(t, err)
		require.ErrorContains(t, err, "failed to read PEM key file")
	})

	t.Run("empty path", func(t *testing.T) {
		t.Parallel()

		err := certs.ValidatePEMKeyFile("")
		require.Error(t, err)
		require.ErrorContains(t, err, "failed to read PEM key file")
	})

	t.Run("non-PEM contents", func(t *testing.T) {
		t.Parallel()

		path := testutil.WriteFile(t, t.TempDir(), "key.pem", []byte("not pem at all"))
		err := certs.ValidatePEMKeyFile(path)
		require.Error(t, err)
		require.ErrorContains(t, err, "no PRIVATE KEY block")
	})

	t.Run("PEM with only certificate block", func(t *testing.T) {
		t.Parallel()

		// A CERTIFICATE block is valid PEM but not a private key.
		path := writePEMFile(t, testutil.RSACert(t, validTemplate()))
		err := certs.ValidatePEMKeyFile(path)
		require.Error(t, err)
		require.ErrorContains(t, err, "no PRIVATE KEY block")
	})

	t.Run("PEM with mixed blocks accepts first PRIVATE KEY", func(t *testing.T) {
		t.Parallel()

		// A CERTIFICATE then a PRIVATE KEY block: the loop should keep scanning
		// past the cert and accept the key.
		certPEM := testutil.RSACert(t, validTemplate())

		_, keyFile := testutil.GenerateSelfSignedCert(t)
		keyPEM := readFile(t, keyFile)

		mixed := append(append([]byte{}, certPEM...), keyPEM...)
		path := testutil.WriteFile(t, t.TempDir(), "mixed.pem", mixed)
		require.NoError(t, certs.ValidatePEMKeyFile(path))
	})
}

func readFile(t *testing.T, path string) []byte {
	t.Helper()
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	return data
}

func writePEMFile(t *testing.T, data []byte) string {
	t.Helper()
	return testutil.WriteFile(t, t.TempDir(), "cert.pem", data)
}

// validKey returns a path to a PEM-encoded private key file. The key contents
// are not parsed by ValidatePEMKeyFile, so any PEM block typed *PRIVATE KEY
// satisfies the check; this helper reuses the ECDSA key produced by testutil to
// avoid duplicating key-generation code.
func validKey(t *testing.T) string {
	t.Helper()
	_, keyFile := testutil.GenerateSelfSignedCert(t)
	return keyFile
}

// rsaCert generates a self-signed RSA certificate with a key of the given bit
// size and returns its PEM encoding. Unlike testutil.RSACert, the key size is
// caller-controlled so tests can exercise below-minimum keys.
func rsaCert(t *testing.T, bits int) []byte {
	t.Helper()

	key, err := rsa.GenerateKey(rand.Reader, bits)
	require.NoError(t, err)

	tmpl := validTemplate()
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	require.NoError(t, err)

	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
}

// ecdsaCert generates a self-signed ECDSA certificate over the given curve and
// returns its PEM encoding, allowing tests to exercise below-minimum curves.
func ecdsaCert(t *testing.T, curve elliptic.Curve) []byte {
	t.Helper()

	key, err := ecdsa.GenerateKey(curve, rand.Reader)
	require.NoError(t, err)

	tmpl := validTemplate()
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	require.NoError(t, err)

	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
}

// ed25519Cert generates a self-signed Ed25519 certificate and returns its PEM
// encoding. Ed25519 is unsupported by SufficientKeySize, so this exercises the
// rejection path for key types other than RSA and ECDSA.
func ed25519Cert(t *testing.T) []byte {
	t.Helper()

	pub, key, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)

	tmpl := validTemplate()
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, pub, key)
	require.NoError(t, err)

	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
}

func validTemplate() *x509.Certificate {
	return &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "test"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
	}
}

func caTemplate() *x509.Certificate {
	tmpl := validTemplate()
	tmpl.IsCA = true
	tmpl.BasicConstraintsValid = true
	return tmpl
}
