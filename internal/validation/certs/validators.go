package certs

import (
	"crypto/ecdsa"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"time"

	"github.com/temporalio/temporal-proxy/internal/validation"
)

var (
	// weakAlgorithms lists signature algorithms considered cryptographically broken.
	// SHA-1 and MD5 are vulnerable to collision attacks; MD2 is obsolete; DSA is deprecated in X.509.
	weakAlgorithms = map[x509.SignatureAlgorithm]bool{
		x509.SHA1WithRSA:   true,
		x509.ECDSAWithSHA1: true,
		x509.MD2WithRSA:    true,
		x509.MD5WithRSA:    true,
		x509.DSAWithSHA1:   true,
		x509.DSAWithSHA256: true,
	}

	// suiteKeyTypes maps each supported TLS cipher suite to the key type it requires ("rsa" or
	// "ecdsa"). Used by SecureAlgorithm to verify that a certificate's public key is compatible
	// with the caller's allowed suite set.
	suiteKeyTypes = map[uint16]string{
		// ECDHE_RSA suites
		tls.TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA:          "rsa",
		tls.TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA:          "rsa",
		tls.TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA256:       "rsa",
		tls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256:       "rsa",
		tls.TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384:       "rsa",
		tls.TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305_SHA256: "rsa",
		// ECDHE_ECDSA suites
		tls.TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA:          "ecdsa",
		tls.TLS_ECDHE_ECDSA_WITH_AES_256_CBC_SHA:          "ecdsa",
		tls.TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA256:       "ecdsa",
		tls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256:       "ecdsa",
		tls.TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384:       "ecdsa",
		tls.TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305_SHA256: "ecdsa",
	}
)

// NotExpired returns a Validator that fails if a certificate is expired or not yet valid.
func NotExpired() Validator {
	return func(cert *x509.Certificate) error {
		now := time.Now()
		if now.Before(cert.NotBefore) {
			return validation.Error{
				Subject: cert.Subject.CommonName,
				Field:   "expiry",
				Message: fmt.Sprintf("certificate not yet valid until %s", cert.NotBefore.Format(time.RFC3339)),
			}
		}

		if now.After(cert.NotAfter) {
			return validation.Error{
				Subject: cert.Subject.CommonName,
				Field:   "expiry",
				Message: fmt.Sprintf("certificate expired at %s", cert.NotAfter.Format(time.RFC3339)),
			}
		}

		return nil
	}
}

// IsCA returns a Validator that fails if a certificate is not a CA certificate.
func IsCA() Validator {
	return func(cert *x509.Certificate) error {
		if !cert.IsCA {
			return validation.Error{
				Subject: cert.Subject.CommonName,
				Field:   "is_ca",
				Message: "certificate is not a CA",
			}
		}

		return nil
	}
}

// SecureAlgorithm returns a Validator that fails if a certificate uses a weak signature algorithm.
// If allowedSuites is provided, it also fails if the certificate's key type is incompatible with
// all of the supplied TLS cipher suites.
func SecureAlgorithm(allowedSuites ...uint16) Validator {
	return func(cert *x509.Certificate) error {
		if weakAlgorithms[cert.SignatureAlgorithm] {
			return validation.Error{
				Subject: cert.Subject.CommonName,
				Field:   "signature_algorithm",
				Message: fmt.Sprintf("weak signature algorithm: %s", cert.SignatureAlgorithm),
			}
		}

		if len(allowedSuites) == 0 {
			return nil
		}

		allowed := make(map[string]bool)
		for _, suite := range allowedSuites {
			if kt, ok := suiteKeyTypes[suite]; ok {
				allowed[kt] = true
			}
		}

		if len(allowed) == 0 {
			return nil
		}

		kt := keyTypeOf(cert.PublicKey)
		if !allowed[kt] {
			return validation.Error{
				Subject: cert.Subject.CommonName,
				Field:   "key_type",
				Message: fmt.Sprintf("key type %q incompatible with allowed cipher suites", kt),
			}
		}

		return nil
	}
}

// CAChain returns a Validator that fails if a certificate cannot be verified against the provided
// PEM-encoded CA certificate(s).
func CAChain(caPEM []byte) Validator {
	pool := x509.NewCertPool()
	poolOK := pool.AppendCertsFromPEM(caPEM)

	return func(cert *x509.Certificate) error {
		if !poolOK {
			return validation.Error{
				Subject: cert.Subject.CommonName,
				Field:   "ca_chain",
				Message: "failed to parse CA PEM",
			}
		}

		if _, err := cert.Verify(x509.VerifyOptions{
			Roots:     pool,
			KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageAny},
		}); err != nil {
			return validation.Error{
				Subject: cert.Subject.CommonName,
				Field:   "ca_chain",
				Message: fmt.Sprintf("certificate does not chain to provided CA: %s", err),
			}
		}

		return nil
	}
}

func keyTypeOf(pub any) string {
	switch pub.(type) {
	case *rsa.PublicKey:
		return "rsa"
	case *ecdsa.PublicKey:
		return "ecdsa"
	default:
		return "unknown"
	}
}
