package creds

import (
	"crypto/tls"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

// Listener is a server-side credential resolved from a set of [Option]s. It
// owns the inbound TLS-mode decision (insecure, server TLS, or mutual TLS) and
// builds the corresponding [google.golang.org/grpc.ServerOption].
type Listener struct {
	opts     *options
	mode     Mode
	material *material
}

// NewListener builds a server-side credential from opts. With no material
// options it is a server-TLS listener that requires its own certificate.
// Construction performs no file I/O and never fails; illegal combinations and
// file problems surface at Validate and ServerOption.
func NewListener(opts ...Option) *Listener {
	o := newOptions(opts)
	mode := resolveServer(o)

	return &Listener{opts: o, mode: mode, material: newMaterial(o, mode)}
}

// Mode reports the resolved inbound TLS mode.
func (l *Listener) Mode() Mode {
	return l.mode
}

// Validate checks the credential at configuration time without binding. It
// first checks the cross-field legality of the configuration, then (for modes
// that reference files) reads and inspects the certificate material.
func (l *Listener) Validate() error {
	if err := l.opts.validateServer(); err != nil {
		return err
	}

	return validateMaterial(l.opts, l.mode)
}

// ServerOption returns the [grpc.ServerOption] for inbound connections, built
// from the same configuration [Listener.TLSConfig] returns.
func (l *Listener) ServerOption() (grpc.ServerOption, error) {
	cfg, err := l.TLSConfig()
	if err != nil {
		return nil, err
	}

	if cfg == nil {
		return grpc.Creds(insecure.NewCredentials()), nil
	}

	return grpc.Creds(credentials.NewTLS(cfg)), nil
}

// TLSConfig returns the server TLS configuration for inbound connections, or nil
// for the insecure mode. The same legality guard as Validate runs first. Each
// handshake builds its configuration from the material's current snapshot via
// GetConfigForClient, so the returned config carries no certificates itself.
// Server TLS presents the configured certificate; mutual TLS additionally
// requires and verifies client certificates against the configured CA. Both
// require at least TLS 1.2 and restrict TLS 1.2 sessions to the preferred
// AES-GCM cipher suites.
func (l *Listener) TLSConfig() (*tls.Config, error) {
	if err := l.opts.validateServer(); err != nil {
		return nil, err
	}

	if l.mode == ModeInsecure {
		return nil, nil
	}

	if err := l.material.load(); err != nil {
		return nil, err
	}

	return &tls.Config{
		MinVersion:   minTLSVersion,
		CipherSuites: preferredCipherSuites,
		GetConfigForClient: func(*tls.ClientHelloInfo) (*tls.Config, error) {
			return serverConfig(l.material.current(), l.mode), nil
		},
	}, nil
}

// Encrypted reports whether the inbound transport is encrypted. Only the
// insecure mode is unencrypted.
func (l *Listener) Encrypted() bool {
	return l.mode != ModeInsecure
}

// serverConfig builds the inbound TLS configuration for one handshake. The
// returned config replaces the base one entirely, so NextProtos is pinned here:
// the ALPN list grpc-go and net/http add to the base config never reaches it.
func serverConfig(s *snapshot, mode Mode) *tls.Config {
	cfg := &tls.Config{
		Certificates: []tls.Certificate{*s.cert},
		MinVersion:   minTLSVersion,
		CipherSuites: preferredCipherSuites,
		NextProtos:   []string{"h2", "http/1.1"},
	}

	if mode == ModeMutualTLS {
		cfg.ClientAuth = tls.RequireAndVerifyClientCert
		cfg.ClientCAs = s.pool
	}

	return cfg
}
