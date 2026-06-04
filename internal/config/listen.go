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

func (t *TLSConfig) Validate() error {
	return validation.Validate(
		"",
		validation.WhenRules(
			func() bool { return t.CA != "" },
			validation.Nested(
				"", creds.NewMTLS(t.CA, t.Cert, t.Key, creds.MTLSOptions{
					ServerName: t.ServerName,
				}),
			),
		),
		validation.WhenRules(
			func() bool { return t.CA == "" },
			validation.Nested("", creds.NewServerTLS(t.Cert, t.Key)),
		),
	)
}
