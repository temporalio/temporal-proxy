package creds

import (
	"crypto/tls"
	"fmt"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

// Listener is a server-side credential resolved from a set of [Option]s. It
// owns the inbound TLS-mode decision (insecure, server TLS, or mutual TLS) and
// builds the corresponding [google.golang.org/grpc.ServerOption].
type Listener struct {
	opts *options
	mode Mode
}

// NewListener builds a server-side credential from opts. With no material
// options it is a server-TLS listener that requires its own certificate.
// Construction performs no file I/O and never fails; illegal combinations and
// file problems surface at Validate and ServerOption.
func NewListener(opts ...Option) *Listener {
	o := newOptions(opts)

	return &Listener{opts: o, mode: resolveServer(o)}
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

// ServerOption returns the [grpc.ServerOption] for inbound connections. The same
// legality guard as Validate runs first. Server TLS presents the configured
// certificate; mutual TLS additionally requires and verifies client
// certificates against the configured CA. Both require at least TLS 1.2 and
// restrict TLS 1.2 sessions to the preferred AES-GCM cipher suites.
func (l *Listener) ServerOption() (grpc.ServerOption, error) {
	if err := l.opts.validateServer(); err != nil {
		return nil, err
	}

	if l.mode == ModeInsecure {
		return grpc.Creds(insecure.NewCredentials()), nil
	}

	cert, err := tls.LoadX509KeyPair(l.opts.cert, l.opts.key)
	if err != nil {
		return nil, fmt.Errorf("failed to load server key pair: %w", err)
	}

	cfg := &tls.Config{
		Certificates: []tls.Certificate{cert},
		MinVersion:   minTLSVersion,
		CipherSuites: preferredCipherSuites,
	}

	if l.mode == ModeMutualTLS {
		pool, err := loadCAPool(l.opts.ca)
		if err != nil {
			return nil, err
		}

		cfg.ClientAuth = tls.RequireAndVerifyClientCert
		cfg.ClientCAs = pool
	}

	return grpc.Creds(credentials.NewTLS(cfg)), nil
}

// Encrypted reports whether the inbound transport is encrypted. Only the
// insecure mode is unencrypted.
func (l *Listener) Encrypted() bool {
	return l.mode != ModeInsecure
}
