package creds

import (
	"crypto/ecdsa"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/temporalio/temporal-proxy/pkg/validation"
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

// Validator inspects a single parsed certificate and returns a
// validation.Error describing any failure, or nil if the certificate is valid.
type Validator validation.Check[*x509.Certificate]

// ValidatePEMFile reads path and forwards its contents to ValidatePEM. A read
// failure is returned wrapped so callers can distinguish IO problems from
// validation failures; the wrapped os.ReadFile error already includes the path.
func ValidatePEMFile(path string, validators ...Validator) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("failed to read PEM file: %w", err)
	}

	return ValidatePEM(data, validators...)
}

// ValidatePEMKeyFile reads path and verifies it contains at least one PEM
// block whose type ends in "PRIVATE KEY" (covering "RSA PRIVATE KEY",
// "EC PRIVATE KEY", and "PRIVATE KEY" for PKCS#8). The block contents are
// not parsed; cryptographic validity and cert/key matching are exercised
// at runtime by [crypto/tls.LoadX509KeyPair].
func ValidatePEMKeyFile(path string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("failed to read PEM key file: %w", err)
	}

	for rest := data; len(rest) > 0; {
		block, r := pem.Decode(rest)
		if block == nil {
			break
		}

		if strings.HasSuffix(block.Type, "PRIVATE KEY") {
			return nil
		}

		rest = r
	}

	return errors.New("no PRIVATE KEY block found in PEM data")
}

// ValidatePEM parses all CERTIFICATE blocks from pemData and runs each
// validator against every parsed certificate, collecting all failures into an
// Errors value. Returns nil immediately when no validators are provided.
func ValidatePEM(pemData []byte, validators ...Validator) error {
	// With no validators there is nothing to check; skip PEM parsing entirely.
	if len(validators) == 0 {
		return nil
	}

	parsed, err := parsePEM(pemData)
	if err != nil {
		return err
	}

	var errs validation.Errors
	for _, cert := range parsed {
		for _, v := range validators {
			if vErr := v(cert); vErr != nil {
				if ve, ok := errors.AsType[validation.Error](vErr); ok {
					errs = append(errs, ve)
					continue
				}

				errs = append(errs, validation.Error{
					Subject: cert.Subject.CommonName,
					Field:   "unknown",
					Message: vErr.Error(),
				})
			}
		}
	}

	if len(errs) > 0 {
		return errs
	}

	return nil
}

// CertificateNotExpired returns a CertificateValidator that rejects
// certificates whose NotBefore is in the future or whose NotAfter is in the
// past.
func CertificateNotExpired() Validator {
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

// IsCACertificate returns a CertificateValidator that rejects certificates
// that do not have the CA basic constraint set.
func IsCACertificate() Validator {
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

// UsesSecureCertificateAlgorithm returns a CertificateValidator that rejects
// certificates signed with known-weak algorithms (SHA-1, MD5, MD2, DSA). When
// allowedSuites are provided it additionally rejects certificates whose public
// key type is incompatible with every suite in that list.
func UsesSecureCertificateAlgorithm(allowedSuites ...uint16) Validator {
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

func parsePEM(data []byte) ([]*x509.Certificate, error) {
	var block *pem.Block
	var parsed []*x509.Certificate

	rest := data
	for len(rest) > 0 {
		block, rest = pem.Decode(rest)
		if block == nil {
			break
		}

		if block.Type != "CERTIFICATE" {
			continue
		}

		cert, err := x509.ParseCertificate(block.Bytes)
		if err != nil {
			return nil, fmt.Errorf("failed to parse certificate: %w", err)
		}

		parsed = append(parsed, cert)
	}

	if len(parsed) == 0 {
		return nil, errors.New("no certificates found in PEM data")
	}

	return parsed, nil
}
