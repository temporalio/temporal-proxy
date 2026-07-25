package creds

import (
	"crypto/tls"

	"github.com/temporalio/temporal-proxy/pkg/validation"
	"github.com/temporalio/temporal-proxy/pkg/validation/certs"
)

// minTLSVersion is the minimum TLS version accepted on both client and server
// connections. TLS 1.2 is the lowest version still considered secure for
// production workloads.
const minTLSVersion = tls.VersionTLS12

// preferredCipherSuites lists the only TLS 1.2 cipher suites accepted on
// server connections. All use ECDHE for forward secrecy and AES-GCM for
// authenticated encryption; both RSA and ECDSA leaf keys are supported so the
// proxy accepts identity material from either family. TLS 1.3 cipher suites
// are not controlled by this field and are always negotiated by the Go
// runtime.
var preferredCipherSuites = []uint16{
	tls.TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,
	tls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,
	tls.TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384,
	tls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256,
}

// validateMaterial reads and inspects the certificate files a mode references.
// It runs after legality, so cert/key pairing and CA presence are already
// guaranteed for the mode in question. Modes with no files (insecure, system
// roots) validate trivially.
func validateMaterial(o *options, mode Mode) error {
	switch mode {
	case ModeCustomCA:
		// A client trust anchor may be a pinned self-signed leaf, so it is not
		// required to carry the CA basic constraint.
		return validation.Validate(
			"",
			validation.Field("ca", o.ca, trustAnchorChecks),
		)
	case ModeServerTLS:
		return validation.Validate(
			"",
			validation.Field("cert", o.cert, leafChecks),
			validation.Field("key", o.key, certs.ValidatePEMKeyFile),
		)
	case ModeMutualTLS:
		return validation.Validate(
			"",
			validation.Field("cert", o.cert, leafChecks),
			validation.Field("key", o.key, certs.ValidatePEMKeyFile),
			validation.Field("ca", o.ca, caChecks),
		)
	default: // ModeInsecure, ModeSystemTLS
		return nil
	}
}

// leafChecks validates a presented certificate: unexpired, signed with a strong
// algorithm, using a key type compatible with the allowed cipher suites, and of
// a sufficient size.
func leafChecks(path string) error {
	return certs.ValidatePEMFile(
		path,
		certs.NotExpired(),
		certs.SecureAlgorithm(preferredCipherSuites...),
		certs.SufficientKeySize(),
	)
}

// caChecks validates a certificate authority: unexpired, an actual CA, signed
// with a strong algorithm, and of a sufficient size.
func caChecks(path string) error {
	return certs.ValidatePEMFile(
		path,
		certs.NotExpired(),
		certs.IsCA(),
		certs.SecureAlgorithm(),
		certs.SufficientKeySize(),
	)
}

// trustAnchorChecks validates a client-side trust anchor: unexpired, signed with
// a strong algorithm, and of a sufficient size. Unlike caChecks it does not
// require the CA basic constraint, so a pinned self-signed leaf is accepted.
func trustAnchorChecks(path string) error {
	return certs.ValidatePEMFile(
		path,
		certs.NotExpired(),
		certs.SecureAlgorithm(),
		certs.SufficientKeySize(),
	)
}
