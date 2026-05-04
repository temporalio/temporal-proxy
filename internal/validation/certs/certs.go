package certs

import (
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"

	"github.com/temporalio/temporal-proxy/internal/validation"
)

// Validator checks a single certificate and returns a validation.Error if it fails.
type Validator func(*x509.Certificate) error

// Validate parses the PEM-encoded certificate data and runs each validator against every
// certificate found. It returns a validation.Errors containing all failures, or nil if all pass.
func Validate(pemData []byte, validators ...Validator) error {
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
