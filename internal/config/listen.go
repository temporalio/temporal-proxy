package config

import (
	"github.com/temporalio/temporal-proxy/internal/transport/creds"
	"github.com/temporalio/temporal-proxy/pkg/validation"
)

type (
	// ListenConfig defines properties for a dial or listen target. TLS is the
	// default for anything the proxy dials: with no TLS block the peer is
	// verified against the system root pool, and Insecure is how an operator
	// deliberately asks for plaintext. A listener has no such default, since it
	// has no certificate to present, so it stays plaintext until a TLS block
	// supplies one.
	ListenConfig struct {
		HostPort string     `yaml:"hostPort"`
		Insecure bool       `yaml:"insecure"`
		TLS      *TLSConfig `yaml:"tls"`
	}

	// TLSConfig specifies the TLS material for one target, and reads differently
	// by direction: a listener presents Cert and Key, and a CA additionally
	// requires each client to present a certificate signed by it (mutual TLS),
	// while a dialer verifies its peer against a CA rather than the system roots
	// and presents Cert and Key only for mutual TLS.
	//
	// NB: Be sure to set ServerName when the host name you dial doesn't match the
	// CN or SAN on the server's certificate.
	TLSConfig struct {
		CA         string `yaml:"ca"`         // PEM-encoded CA certificate
		Cert       string `yaml:"cert"`       // PEM-encoded certificate to present
		Key        string `yaml:"key"`        // PEM-encoded private key
		ServerName string `yaml:"serverName"` // Optional SNI override, when dialing
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
		l.insecureRule(),
	)
}

// Listener resolves the inbound (server) credential for this target. A listener
// has no certificate to fall back on, so it serves plaintext until a TLS block
// supplies one.
func (l *ListenConfig) Listener() *creds.Listener {
	if l.Insecure {
		return creds.NewListener(creds.Insecure())
	}

	return l.TLS.listener()
}

// Dialer resolves the outbound (client) credential for this target. Insecure
// yields plaintext; otherwise the TLS block decides, and an absent one verifies
// the peer against the system root pool.
func (l *ListenConfig) Dialer() *creds.Dialer {
	if l.Insecure {
		return creds.NewDialer(creds.Insecure())
	}

	return l.TLS.dialer()
}

// insecureRule rejects opting out of transport security while also supplying TLS
// material. The two say opposite things, and picking either one silently would
// leave an operator believing the other. It is shared by every role, since the
// contradiction does not depend on the direction of the connection.
func (l *ListenConfig) insecureRule() validation.Rule {
	return validation.WhenRules(
		func() bool { return l.Insecure && l.TLS != nil },
		func() validation.Errors {
			return validation.Errors{{Field: "insecure", Message: "cannot be set together with tls"}}
		},
	)
}

// Validate checks the inbound (listener) TLS material. It delegates to the
// resolved server credential, which owns the mode decision (server TLS vs mutual
// TLS) and the certificate file checks.
func (t *TLSConfig) Validate() error {
	return t.listener().Validate()
}

// validateOutbound validates the config as client-side TLS used to dial an
// upstream. It delegates to the resolved client credential, which owns the mode
// decision (system-root, custom-CA, or mutual TLS) and the legality and file
// checks. Callers must invoke this only when the receiver is non-nil.
func (t *TLSConfig) validateOutbound() validation.Errors {
	return validation.Nested("tls", t.dialer())()
}

// listener resolves the inbound (server) credential for this TLS block. A nil
// receiver has no certificate to present, so it yields an insecure listener.
func (t *TLSConfig) listener() *creds.Listener {
	if t == nil {
		return creds.NewListener(creds.Insecure())
	}

	return creds.NewListener(t.credsOptions()...)
}

// dialer resolves the outbound (client) credential for this TLS block. A nil
// receiver supplies no material, which the client resolves to system-root TLS.
func (t *TLSConfig) dialer() *creds.Dialer {
	return creds.NewDialer(t.credsOptions()...)
}

// credsOptions maps the TLS block onto a set of creds options shared by both
// roles; the caller picks the role via creds.NewListener or creds.NewDialer. A
// nil block, or a present block with no CA and no client certificate, supplies
// no material and leaves the mode to the role.
func (t *TLSConfig) credsOptions() []creds.Option {
	if t == nil {
		return nil
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
