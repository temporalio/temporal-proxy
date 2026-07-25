package creds

import (
	"crypto/tls"
	"fmt"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

// Dialer is a client-side credential resolved from a set of [Option]s. It owns
// the outbound TLS-mode decision (insecure, system-root TLS, custom-CA TLS, or
// mutual TLS) and builds the corresponding
// [google.golang.org/grpc.DialOption]. Certificate and CA material is parsed
// once and reused, so a single Dialer backs every per-request dial of a
// templated upstream.
type Dialer struct {
	opts     *options
	mode     Mode
	material *material
}

// NewDialer builds a client-side credential from opts. With no material options
// it verifies the peer against the system root pool. Construction performs no
// file I/O and never fails; illegal combinations and file problems surface at
// Validate and DialOption.
func NewDialer(opts ...Option) *Dialer {
	o := newOptions(opts)
	mode := resolveClient(o)

	return &Dialer{opts: o, mode: mode, material: &material{opts: o, mode: mode}}
}

// Mode reports the resolved outbound TLS mode.
func (d *Dialer) Mode() Mode {
	return d.mode
}

// Validate checks the credential at configuration time without dialing. It
// first checks the cross-field legality of the configuration, then (for modes
// that reference files) reads and inspects the certificate material.
func (d *Dialer) Validate() error {
	if err := d.opts.validateClient(); err != nil {
		return err
	}

	return validateMaterial(d.opts, d.mode)
}

// DialOption returns the [grpc.DialOption] for outbound connections. serverName
// sets the SNI and hostname-verification name (empty uses the dial target's
// host). The certificate and CA material is parsed on the first call and reused
// on subsequent calls, so only serverName varies per call. The same legality
// guard as Validate runs first, so an illegal configuration fails here rather
// than dialing with the wrong mode.
func (d *Dialer) DialOption(serverName string) (grpc.DialOption, error) {
	if err := d.opts.validateClient(); err != nil {
		return nil, err
	}

	switch d.mode {
	case ModeInsecure:
		return grpc.WithTransportCredentials(insecure.NewCredentials()), nil
	case ModeSystemTLS:
		return grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{
			ServerName: serverName,
			MinVersion: minTLSVersion,
		})), nil
	case ModeCustomCA:
		if err := d.material.load(); err != nil {
			return nil, err
		}

		return grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{
			ServerName: serverName,
			MinVersion: minTLSVersion,
			RootCAs:    d.material.caPool,
		})), nil
	case ModeMutualTLS:
		if err := d.material.load(); err != nil {
			return nil, err
		}

		return grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{
			Certificates: []tls.Certificate{d.material.cert},
			RootCAs:      d.material.caPool,
			ServerName:   serverName,
			MinVersion:   minTLSVersion,
		})), nil
	default:
		return nil, fmt.Errorf("creds: unreachable dialer mode %d", d.mode)
	}
}
