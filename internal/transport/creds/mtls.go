package creds

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	"github.com/temporalio/temporal-proxy/pkg/validation"
)

type (
	// MTLS enables mutual TLS on gRPC connections. Both the client and server
	// present certificates, and each side verifies the other against a shared CA.
	// Minimum TLS version is 1.2.
	MTLS struct {
		caFile     string
		certFile   string
		keyFile    string
		serverName string
		loader     *CertLoader
	}

	// MTLSOptions holds optional configuration for [MTLS] connections.
	MTLSOptions struct {
		// ServerName overrides the server name used to verify the server's
		// certificate hostname. Useful when the server's certificate CN does not
		// match its dial address.
		ServerName string

		// Loader, when set, supplies the client certificate and CA pool used by
		// [MTLS.DialOption]. Share one loader across the short-lived MTLS values
		// built per request for a templated upstream so the certificate and CA
		// files are read and parsed once rather than on every request. When nil,
		// the MTLS loads (and caches) its own material on first use.
		Loader *CertLoader
	}

	// CertLoader reads and parses a client certificate/key pair and a CA
	// certificate pool once, caching the result for reuse across [MTLS] values.
	// It is safe for concurrent use: the parsed material is immutable after the
	// first load, so a cached CA pool and certificate may be shared by many
	// dial options. A rotated certificate on disk is not picked up until the
	// process restarts, matching how a fixed-address upstream loads its material
	// once at startup.
	CertLoader struct {
		caFile   string
		certFile string
		keyFile  string

		once   sync.Once
		cert   tls.Certificate
		caPool *x509.CertPool
		err    error
	}
)

// NewMTLS returns an [MTLS] credential that loads the CA certificate from
// caFile and the client certificate/key pair from certFile and keyFile. opts
// provides additional configuration; a zero-value [MTLSOptions] is valid.
func NewMTLS(caFile, certFile, keyFile string, opts MTLSOptions) *MTLS {
	loader := opts.Loader
	if loader == nil {
		loader = NewCertLoader(caFile, certFile, keyFile)
	}

	return &MTLS{
		caFile:     caFile,
		certFile:   certFile,
		keyFile:    keyFile,
		serverName: opts.ServerName,
		loader:     loader,
	}
}

// NewCertLoader returns a [CertLoader] that loads the CA certificate from caFile
// and the client certificate/key pair from certFile and keyFile on first use.
func NewCertLoader(caFile, certFile, keyFile string) *CertLoader {
	return &CertLoader{caFile: caFile, certFile: certFile, keyFile: keyFile}
}

// DialOption returns a [grpc.DialOption] that configures the outbound
// connection with mutual TLS. The client presents its certificate/key pair and
// verifies the server against the CA pool loaded from caFile.
func (c *MTLS) DialOption() (grpc.DialOption, error) {
	cert, ca, err := c.loader.load()
	if err != nil {
		return nil, err
	}

	return grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{
		Certificates: []tls.Certificate{cert},
		MinVersion:   minTLSVersion,
		RootCAs:      ca,
		ServerName:   c.serverName,
	})), nil
}

// ServerOption returns a [grpc.ServerOption] that configures the server to
// require and verify client certificates against the CA pool loaded from
// caFile. The server uses AES-256-GCM and AES-128-GCM cipher suites and
// requires at least TLS 1.2.
func (c *MTLS) ServerOption() (grpc.ServerOption, error) {
	cert, err := tls.LoadX509KeyPair(c.certFile, c.keyFile)
	if err != nil {
		return nil, fmt.Errorf("failed to load server key pair: %w", err)
	}

	ca := x509.NewCertPool()
	caBytes, err := os.ReadFile(c.caFile)
	if err != nil {
		return nil, fmt.Errorf("failed to load CA certificate: %w", err)
	}

	if ok := ca.AppendCertsFromPEM(caBytes); !ok {
		return nil, fmt.Errorf("failed to parse CA file: %s", c.caFile)
	}

	return grpc.Creds(credentials.NewTLS(&tls.Config{
		Certificates: []tls.Certificate{cert},
		ClientAuth:   tls.RequireAndVerifyClientCert,
		ClientCAs:    ca,
		MinVersion:   minTLSVersion,
		CipherSuites: preferredCipherSuites,
	})), nil
}

// Validate checks both the leaf certificate and the CA certificate for
// configuration problems. The leaf must be unexpired, signed with a strong
// algorithm, use a key type compatible with [preferredCipherSuites], and have a
// sufficiently large key; the CA must be unexpired, have the CA basic
// constraint set, be signed with a strong algorithm, and have a sufficiently
// large key. Both files are always checked; failures are collected into a
// single [validation.Errors] so callers see every problem in one call.
func (c *MTLS) Validate() error {
	return validation.Validate(
		"",
		validation.Field("cert", c.certFile, func(path string) error {
			return ValidatePEMFile(
				path,
				CertificateNotExpired(),
				UsesSecureCertificateAlgorithm(preferredCipherSuites...),
				HasSufficientKeySize(),
			)
		}),
		validation.Field("key", c.keyFile, ValidatePEMKeyFile),
		validation.Field("ca", c.caFile, func(path string) error {
			return ValidatePEMFile(
				path,
				CertificateNotExpired(),
				IsCACertificate(),
				UsesSecureCertificateAlgorithm(),
				HasSufficientKeySize(),
			)
		}),
	)
}

// Encrypted reports whether the transport is encrypted. mTLS always encrypts the
// transport; InsecureSkipVerify weakens peer verification but does not disable
// encryption, so it returns true.
func (c *MTLS) Encrypted() bool {
	return true
}

// load reads and parses the client certificate/key pair and CA pool on the first
// call, caching the result (or the error) for every subsequent call. The
// returned certificate and CA pool are immutable and safe to share across dial
// options and goroutines.
func (l *CertLoader) load() (tls.Certificate, *x509.CertPool, error) {
	l.once.Do(func() {
		cert, err := tls.LoadX509KeyPair(l.certFile, l.keyFile)
		if err != nil {
			l.err = fmt.Errorf("failed to load client key pair: %w", err)
			return
		}

		ca := x509.NewCertPool()
		caBytes, err := os.ReadFile(l.caFile)
		if err != nil {
			l.err = fmt.Errorf("failed to load CA certificate: %w", err)
			return
		}

		if ok := ca.AppendCertsFromPEM(caBytes); !ok {
			l.err = fmt.Errorf("failed to parse CA file: %s", l.caFile)
			return
		}

		l.cert = cert
		l.caPool = ca
	})

	return l.cert, l.caPool, l.err
}
