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

// Listener resolves the inbound (server) credential for this TLS block. A nil
// receiver yields an insecure listener.
func (t *TLSConfig) Listener() *creds.Listener {
	return creds.NewListener(t.credsOptions()...)
}

// Dialer resolves the outbound (client) credential for this TLS block. A nil
// receiver yields an insecure dialer.
func (t *TLSConfig) Dialer() *creds.Dialer {
	return creds.NewDialer(t.credsOptions()...)
}

// Validate checks the inbound (listener) TLS material. It delegates to the
// resolved server credential, which owns the mode decision (server TLS vs mutual
// TLS) and the certificate file checks.
func (t *TLSConfig) Validate() error {
	return t.Listener().Validate()
}

// validateOutbound validates the config as client-side TLS used to dial an
// upstream. It delegates to the resolved client credential, which owns the mode
// decision (system-root, custom-CA, or mutual TLS) and the legality and file
// checks. Callers must invoke this only when the receiver is non-nil.
func (t *TLSConfig) validateOutbound() validation.Errors {
	return validation.Nested("tls", t.Dialer())()
}

// credsOptions maps the TLS block onto a set of creds options shared by both
// roles; the caller picks the role via creds.NewListener or creds.NewDialer. A
// nil block is plaintext. A present block with no CA and no client certificate
// resolves to system-root TLS for a client.
func (t *TLSConfig) credsOptions() []creds.Option {
	if t == nil {
		return []creds.Option{creds.Insecure()}
	}

	var opts []creds.Option
	if t.CA != "" {
		opts = append(opts, creds.WithCA(t.CA))
	}

	if t.Cert != "" || t.Key != "" {
		opts = append(opts, creds.WithCertificate(t.Cert, t.Key))
	}

	return opts
}
