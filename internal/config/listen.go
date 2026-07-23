package config

import (
	"github.com/temporalio/temporal-proxy/internal/transport/creds"
	"github.com/temporalio/temporal-proxy/pkg/validation"
)

type (
	// ListenConfig defines properties for an inbound listener.
	ListenConfig struct {
		HostPort string     `yaml:"hostPort"`
		TLS      *TLSConfig `yaml:"tls"`
	}

	// TLSConfig specifies TLS material for an inbound HTTPS listener. When CAFile
	// is non-empty the listener enforces mutual TLS: connecting clients must
	// present a certificate signed by that CA.
	//
	// NB: Be sure to set ServerName when the host name you dial doesn't match the
	// CN or SAN on the server's certificate.
	TLSConfig struct {
		CA         string `yaml:"ca"`         // PEM-encoded CA certificate (mTLS only)
		Cert       string `yaml:"cert"`       // PEM-encoded server certificate
		Key        string `yaml:"key"`        // PEM-encoded private key
		ServerName string `yaml:"serverName"` // Optional SNI override
	}

	// caTrustAnchor validates a certificate used as an outbound trust anchor
	// (client-side TLS with no client certificate presented). The anchor may be
	// either a CA or a pinned self-signed leaf. It is checked for expiry, a secure
	// signature algorithm, and sufficient key size.
	caTrustAnchor struct {
		caFile string
	}
)

// Validate checks the host:port and, when present, the TLS configuration.
func (l *ListenConfig) Validate() error {
	return validation.Validate(
		"",
		validation.Field("hostPort", l.HostPort, validation.IsHostPort()),
		validation.WhenRules(
			func() bool { return l.TLS != nil },
			validation.Nested("tls", l.TLS),
		),
	)
}

// Validate checks the configured certificate material: mutual TLS when a CA is
// set, otherwise server-only TLS.
func (t *TLSConfig) Validate() error {
	return validation.Validate(
		"",
		validation.WhenRules(
			func() bool { return t.CA != "" },
			validation.Nested("", t.mtlsCreds()),
		),
		validation.WhenRules(
			func() bool { return t.CA == "" },
			validation.Nested("", creds.NewServerTLS(t.Cert, t.Key)),
		),
	)
}

// mtlsCreds builds the mutual-TLS credential described by the config: the CA
// verifies the peer and the client key pair is the presented certificate. It is
// shared by the inbound listener validation and the outbound upstream validation
// so the construction lives in one place.
func (t *TLSConfig) mtlsCreds() *creds.MTLS {
	return creds.NewMTLS(t.CA, t.Cert, t.Key, creds.MTLSOptions{ServerName: t.ServerName})
}

// validateOutbound validates the config as client-side TLS used to dial an
// upstream. A client certificate (cert+key) selects mutual TLS and requires a
// CA; a CA alone verifies the upstream against a private trust anchor while
// presenting no client certificate; neither means client TLS against the system
// root pool. Callers must invoke this only when the receiver is non-nil.
func (t *TLSConfig) validateOutbound() validation.Errors {
	hasCert := t.Cert != "" || t.Key != ""

	switch {
	case (t.Cert == "") != (t.Key == ""):
		return validation.Errors{{Subject: "tls", Message: "cert and key must be set together"}}
	case hasCert && t.CA == "":
		return validation.Errors{{Subject: "tls", Field: "ca", Message: "is required when a client certificate is set"}}
	case hasCert:
		return validation.Nested("tls", t.mtlsCreds())()
	case t.CA != "":
		return validation.Nested("tls", caTrustAnchor{caFile: t.CA})()
	default:
		return nil
	}
}

// Validate checks that the trust anchor is not expired and is signed with a
// strong algorithm and a sufficiently large key.
func (c caTrustAnchor) Validate() error {
	return validation.Validate(
		"",
		validation.Field("ca", c.caFile, func(path string) error {
			return creds.ValidatePEMFile(
				path,
				creds.CertificateNotExpired(),
				creds.UsesSecureCertificateAlgorithm(),
				creds.HasSufficientKeySize(),
			)
		}),
	)
}
