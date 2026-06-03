package creds

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"

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
		skipVerify bool
	}

	// MTLSOptions holds optional configuration for [MTLS] connections.
	MTLSOptions struct {
		// InsecureSkipVerify disables server certificate verification on the
		// client side. This should only be used in testing; never in production.
		InsecureSkipVerify bool

		// ServerName overrides the server name used to verify the server's
		// certificate hostname. Useful when the server's certificate CN does not
		// match its dial address.
		ServerName string
	}
)

// NewMTLS returns an [MTLS] credential that loads the CA certificate from
// caFile and the client certificate/key pair from certFile and keyFile. opts
// provides additional configuration; a zero-value [MTLSOptions] is valid.
func NewMTLS(caFile, certFile, keyFile string, opts MTLSOptions) *MTLS {
	return &MTLS{
		caFile:     caFile,
		certFile:   certFile,
		keyFile:    keyFile,
		serverName: opts.ServerName,
		skipVerify: opts.InsecureSkipVerify,
	}
}

// DialOption returns a [grpc.DialOption] that configures the outbound
// connection with mutual TLS. The client presents its certificate/key pair and
// verifies the server against the CA pool loaded from caFile.
func (c *MTLS) DialOption() (grpc.DialOption, error) {
	cert, err := tls.LoadX509KeyPair(c.certFile, c.keyFile)
	if err != nil {
		return nil, fmt.Errorf("failed to load client key pair: %w", err)
	}

	ca := x509.NewCertPool()
	caBytes, err := os.ReadFile(c.caFile)
	if err != nil {
		return nil, fmt.Errorf("failed to load CA certificate: %w", err)
	}

	if ok := ca.AppendCertsFromPEM(caBytes); !ok {
		return nil, fmt.Errorf("failed to parse CA file: %s", c.caFile)
	}

	return grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{
		Certificates:       []tls.Certificate{cert},
		MinVersion:         minTLSVersion,
		RootCAs:            ca,
		ServerName:         c.serverName,
		InsecureSkipVerify: c.skipVerify,
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
// algorithm, and use a key type compatible with [preferredCipherSuites]; the CA
// must be unexpired, have the CA basic constraint set, and be signed with a
// strong algorithm. Both files are always checked; failures are collected
// into a single [validation.Errors] so callers see every problem in one call.
func (c *MTLS) Validate() error {
	if errs := validation.Validate(
		"",
		validation.Field("cert", c.certFile, func(path string) error {
			return ValidatePEMFile(
				path,
				CertificateNotExpired(),
				UsesSecureCertificateAlgorithm(preferredCipherSuites...),
			)
		}),
		validation.Field("ca", c.caFile, func(path string) error {
			return ValidatePEMFile(
				path,
				CertificateNotExpired(),
				IsCACertificate(),
				UsesSecureCertificateAlgorithm(),
			)
		}),
	); len(errs) > 0 {
		return errs
	}

	return nil
}
