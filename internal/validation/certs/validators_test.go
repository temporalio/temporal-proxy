package certs_test

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/temporalio/temporal-proxy/internal/validation"
	"github.com/temporalio/temporal-proxy/internal/validation/certs"
)

type certConfig struct {
	cn        string
	isCA      bool
	notBefore time.Time
	notAfter  time.Time
	keyType   string // "ecdsa" (default) or "rsa"
	parent    *x509.Certificate
	parentKey any
}

func TestValidate(t *testing.T) {
	t.Parallel()

	t.Run("no validators returns nil for any input", func(t *testing.T) {
		t.Parallel()
		require.NoError(t, certs.Validate([]byte("not pem")))
	})

	t.Run("no certificate blocks returns error", func(t *testing.T) {
		t.Parallel()
		pemData := []byte("-----BEGIN PRIVATE KEY-----\ndGVzdA==\n-----END PRIVATE KEY-----\n")
		alwaysFail := certs.Validator(func(_ *x509.Certificate) error {
			return validation.Error{Subject: "x", Field: "f", Message: "fail"}
		})
		err := certs.Validate(pemData, alwaysFail)
		require.ErrorContains(t, err, "no certificates found")
	})

	t.Run("returns nil when all validators pass", func(t *testing.T) {
		t.Parallel()
		_, _, pemData := mustMakeCert(t, certConfig{cn: "ok"})
		alwaysPass := certs.Validator(func(_ *x509.Certificate) error { return nil })
		require.NoError(t, certs.Validate(pemData, alwaysPass))
	})

	t.Run("collects all errors across all certs", func(t *testing.T) {
		t.Parallel()
		_, _, pem1 := mustMakeCert(t, certConfig{cn: "cert1"})
		_, _, pem2 := mustMakeCert(t, certConfig{cn: "cert2"})
		combined := append(pem1, pem2...)

		alwaysFail := certs.Validator(func(cert *x509.Certificate) error {
			return validation.Error{Subject: cert.Subject.CommonName, Field: "test", Message: "fail"}
		})

		err := certs.Validate(combined, alwaysFail)
		require.Error(t, err)

		var errs validation.Errors
		require.ErrorAs(t, err, &errs)
		require.Len(t, errs, 2)
		require.Equal(t, "cert1", errs[0].Subject)
		require.Equal(t, "cert2", errs[1].Subject)
	})

	t.Run("skips non-certificate PEM blocks", func(t *testing.T) {
		t.Parallel()
		_, _, certPEM := mustMakeCert(t, certConfig{cn: "cert"})
		privKeyBlock := []byte("-----BEGIN PRIVATE KEY-----\ndGVzdA==\n-----END PRIVATE KEY-----\n")
		mixed := append(privKeyBlock, certPEM...)

		alwaysPass := certs.Validator(func(_ *x509.Certificate) error { return nil })
		require.NoError(t, certs.Validate(mixed, alwaysPass))
	})
}

func TestNotExpired(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		notBefore time.Time
		notAfter  time.Time
		wantMsg   string
	}{
		{
			name:      "valid cert",
			notBefore: time.Now().Add(-time.Hour),
			notAfter:  time.Now().Add(time.Hour),
		},
		{
			name:      "expired cert",
			notBefore: time.Now().Add(-2 * time.Hour),
			notAfter:  time.Now().Add(-time.Minute),
			wantMsg:   "expired",
		},
		{
			name:      "not yet valid cert",
			notBefore: time.Now().Add(time.Hour),
			notAfter:  time.Now().Add(2 * time.Hour),
			wantMsg:   "not yet valid",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			_, _, pemData := mustMakeCert(t, certConfig{
				cn:        tc.name,
				notBefore: tc.notBefore,
				notAfter:  tc.notAfter,
			})

			err := certs.Validate(pemData, certs.NotExpired())
			if tc.wantMsg == "" {
				require.NoError(t, err)
				return
			}

			var errs validation.Errors
			require.ErrorAs(t, err, &errs)
			require.Len(t, errs, 1)
			require.Equal(t, "expiry", errs[0].Field)
			require.Equal(t, tc.name, errs[0].Subject)
			require.ErrorContains(t, err, tc.wantMsg)
		})
	}
}

func TestIsCA(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		isCA    bool
		wantErr bool
	}{
		{name: "CA cert", isCA: true, wantErr: false},
		{name: "leaf cert", isCA: false, wantErr: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			_, _, pemData := mustMakeCert(t, certConfig{cn: tc.name, isCA: tc.isCA})

			err := certs.Validate(pemData, certs.IsCA())
			if !tc.wantErr {
				require.NoError(t, err)
				return
			}

			var errs validation.Errors
			require.ErrorAs(t, err, &errs)
			require.Len(t, errs, 1)
			require.Equal(t, "is_ca", errs[0].Field)
			require.Equal(t, tc.name, errs[0].Subject)
		})
	}
}

func TestSecureAlgorithm(t *testing.T) {
	t.Parallel()

	t.Run("ECDSA cert passes with no suites", func(t *testing.T) {
		t.Parallel()
		_, _, pemData := mustMakeCert(t, certConfig{cn: "ecdsa-cert"})
		require.NoError(t, certs.Validate(pemData, certs.SecureAlgorithm()))
	})

	t.Run("weak SHA1 signature algorithm is rejected", func(t *testing.T) {
		t.Parallel()
		// Call the validator directly on a fake cert — avoids x509.CreateCertificate
		// rejecting SHA1 in newer Go versions.
		k, err := rsa.GenerateKey(rand.Reader, 2048)
		require.NoError(t, err)
		fakeCert := &x509.Certificate{
			Subject:            pkix.Name{CommonName: "sha1-cert"},
			SignatureAlgorithm: x509.SHA1WithRSA,
			PublicKey:          &k.PublicKey,
		}
		validatorErr := certs.SecureAlgorithm()(fakeCert)
		var ve validation.Error
		require.ErrorAs(t, validatorErr, &ve)
		require.Equal(t, "signature_algorithm", ve.Field)
		require.Equal(t, "sha1-cert", ve.Subject)
	})

	t.Run("weak MD5 signature algorithm is rejected", func(t *testing.T) {
		t.Parallel()
		k, err := rsa.GenerateKey(rand.Reader, 2048)
		require.NoError(t, err)
		fakeCert := &x509.Certificate{
			Subject:            pkix.Name{CommonName: "md5-cert"},
			SignatureAlgorithm: x509.MD5WithRSA,
			PublicKey:          &k.PublicKey,
		}
		validatorErr := certs.SecureAlgorithm()(fakeCert)
		var ve validation.Error
		require.ErrorAs(t, validatorErr, &ve)
		require.Equal(t, "signature_algorithm", ve.Field)
		require.Equal(t, "md5-cert", ve.Subject)
	})

	t.Run("RSA cert passes with ECDHE_RSA suites", func(t *testing.T) {
		t.Parallel()
		_, _, pemData := mustMakeCert(t, certConfig{cn: "rsa-cert", keyType: "rsa"})
		err := certs.Validate(pemData, certs.SecureAlgorithm(
			tls.TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,
			tls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,
		))
		require.NoError(t, err)
	})

	t.Run("ECDSA cert rejected with RSA-only suites", func(t *testing.T) {
		t.Parallel()
		_, _, pemData := mustMakeCert(t, certConfig{cn: "ecdsa-cert"})

		err := certs.Validate(pemData, certs.SecureAlgorithm(
			tls.TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,
			tls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,
		))
		var errs validation.Errors
		require.ErrorAs(t, err, &errs)
		require.Len(t, errs, 1)
		require.Equal(t, "key_type", errs[0].Field)
	})
}

func TestCAChain(t *testing.T) {
	t.Parallel()

	t.Run("cert signed by CA passes", func(t *testing.T) {
		t.Parallel()
		caCert, caKey, caPEM := mustMakeCert(t, certConfig{cn: "test-ca", isCA: true})
		_, _, leafPEM := mustMakeCert(t, certConfig{cn: "leaf", parent: caCert, parentKey: caKey})

		err := certs.Validate(leafPEM, certs.CAChain(caPEM))
		require.NoError(t, err)
	})

	t.Run("self-signed cert not in CA pool fails", func(t *testing.T) {
		t.Parallel()
		_, _, leafPEM := mustMakeCert(t, certConfig{cn: "self-signed"})
		_, _, differentCAPEM := mustMakeCert(t, certConfig{cn: "other-ca", isCA: true})

		err := certs.Validate(leafPEM, certs.CAChain(differentCAPEM))
		var errs validation.Errors
		require.ErrorAs(t, err, &errs)
		require.Len(t, errs, 1)
		require.Equal(t, "ca_chain", errs[0].Field)
		require.Equal(t, "self-signed", errs[0].Subject)
	})

	t.Run("invalid CA PEM fails", func(t *testing.T) {
		t.Parallel()
		_, _, leafPEM := mustMakeCert(t, certConfig{cn: "leaf"})

		err := certs.Validate(leafPEM, certs.CAChain([]byte("not pem")))
		var errs validation.Errors
		require.ErrorAs(t, err, &errs)
		require.Len(t, errs, 1)
		require.Equal(t, "ca_chain", errs[0].Field)
		require.ErrorContains(t, err, "failed to parse CA PEM")
	})
}

func mustMakeCert(t *testing.T, cfg certConfig) (*x509.Certificate, any, []byte) {
	t.Helper()

	if cfg.cn == "" {
		cfg.cn = "test-cert"
	}

	if cfg.notBefore.IsZero() {
		cfg.notBefore = time.Now().Add(-time.Hour)
	}

	if cfg.notAfter.IsZero() {
		cfg.notAfter = time.Now().Add(time.Hour)
	}

	var privKey any
	var pubKey any

	switch cfg.keyType {
	case "rsa":
		k, err := rsa.GenerateKey(rand.Reader, 2048)
		require.NoError(t, err)
		privKey = k
		pubKey = &k.PublicKey
	default:
		k, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		require.NoError(t, err)
		privKey = k
		pubKey = &k.PublicKey
	}

	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: cfg.cn},
		NotBefore:             cfg.notBefore,
		NotAfter:              cfg.notAfter,
		BasicConstraintsValid: cfg.isCA,
		IsCA:                  cfg.isCA,
	}
	if cfg.isCA {
		tmpl.KeyUsage = x509.KeyUsageCertSign | x509.KeyUsageCRLSign
	} else {
		tmpl.KeyUsage = x509.KeyUsageDigitalSignature
	}

	parent := tmpl
	signerKey := privKey
	if cfg.parent != nil {
		parent = cfg.parent
		signerKey = cfg.parentKey
	}

	der, err := x509.CreateCertificate(rand.Reader, tmpl, parent, pubKey, signerKey)
	require.NoError(t, err)

	cert, err := x509.ParseCertificate(der)
	require.NoError(t, err)

	return cert, privKey, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
}
