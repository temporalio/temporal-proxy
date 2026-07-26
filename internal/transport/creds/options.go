package creds

import "github.com/temporalio/temporal-proxy/pkg/validation"

var errKeyAndCertRequired = validation.Error{Field: "cert", Message: "certificate and key must be set together"}

type (
	// Option configures the TLS material for a [Dialer] or [Listener].
	Option interface {
		apply(*options)
	}

	options struct {
		insecure bool
		ca       string
		cert     string
		key      string
	}

	optFunc func(*options)
)

func (f optFunc) apply(o *options) { f(o) }

// Insecure disables transport security. It is the only way to obtain a
// plaintext credential: security is the default, so an accidentally-empty
// credential fails toward TLS rather than silently downgrading. Use it
// deliberately, for example on the local loopback socket.
func Insecure() Option {
	return optFunc(func(o *options) { o.insecure = true })
}

// WithCA verifies the peer against the CA (or pinned trust anchor) in caFile.
func WithCA(caFile string) Option {
	return optFunc(func(o *options) { o.ca = caFile })
}

// WithCertificate presents the certFile/keyFile key pair. Both must be set
// together; that pairing is enforced when the credential is validated or used.
func WithCertificate(certFile, keyFile string) Option {
	return optFunc(func(o *options) {
		o.cert = certFile
		o.key = keyFile
	})
}

// validateClient reports cross-field problems with an outbound configuration
// before any file is read: a certificate and key must be supplied together, and
// a client certificate requires a CA to verify the peer against. An insecure or
// material-free (system-root) configuration is always legal.
func (o *options) validateClient() error {
	if o.insecure {
		return nil
	}

	if (o.cert == "") != (o.key == "") {
		return errKeyAndCertRequired
	}

	if (o.cert != "" || o.key != "") && o.ca == "" {
		return validation.Error{Field: "ca", Message: "certificate authority is required when a client certificate is set"}
	}

	return nil
}

// validateServer reports cross-field problems with an inbound configuration
// before any file is read: a listener always presents its own certificate, so a
// certificate and key are both required (and must be supplied together). An
// insecure configuration is always legal.
func (o *options) validateServer() error {
	if o.insecure {
		return nil
	}

	if (o.cert == "") != (o.key == "") {
		return errKeyAndCertRequired
	}

	if o.cert == "" || o.key == "" {
		return validation.Error{Field: "cert", Message: "a server certificate is required"}
	}

	return nil
}

func newOptions(opts []Option) *options {
	o := new(options)
	for _, opt := range opts {
		opt.apply(o)
	}

	return o
}
