package creds

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	"github.com/temporalio/temporal-proxy/pkg/validation"
)

// minTLSVersion is the minimum TLS version accepted on both client and server
// connections. TLS 1.2 is the lowest version still considered secure for
// production workloads.
const minTLSVersion = tls.VersionTLS12

// preferredCipherSuites lists the only TLS 1.2 cipher suites accepted on
// server connections. Both use ECDHE for forward secrecy and AES-GCM for
// authenticated encryption. TLS 1.3 cipher suites are not controlled by this
// field and are always negotiated by the Go runtime.
var preferredCipherSuites = []uint16{
	tls.TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,
	tls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,
}

type (
	// TLS enables one-way (server-side) TLS on gRPC connections. The server
	// presents a certificate to the client; the client is not required to present
	// one. Minimum TLS version is 1.2.
	TLS struct {
		certFile   string
		keyFile    string
		serverName string
		caFile     string
		caLoader   *CAPoolLoader
	}

	// CAPoolLoader reads and parses a CA certificate pool once, caching the result
	// for reuse across the [TLS] values built per request for a templated
	// upstream. It is safe for concurrent use: the parsed pool is immutable after
	// the first load, so it may be shared by many dial options. A rotated CA on
	// disk is not picked up until the process restarts, matching how a
	// fixed-address upstream loads its material once at startup. This is the
	// CA-only analogue of [CertLoader], which also caches a client key pair for
	// mutual TLS.
	CAPoolLoader struct {
		caFile string

		once sync.Once
		pool *x509.CertPool
		err  error
	}
)

// NewClientTLS returns a [TLS] credential suitable for outbound (client-side)
// connections. The server's certificate is verified against the system root CA
// pool; no client certificate is presented. Use [NewMTLS] when mutual
// authentication is required. serverName overrides the name used for SNI and
// certificate verification; when empty it defaults to the dial target's host.
func NewClientTLS(serverName string) *TLS {
	return &TLS{serverName: serverName}
}

// NewClientTLSWithCA returns a [TLS] credential for outbound (client-side)
// connections that verifies the server's certificate against the CA loaded from
// caFile instead of the system root pool. No client certificate is presented;
// use [NewMTLS] when the upstream requires mutual authentication. serverName
// overrides the name used for SNI and verification; when empty it defaults to
// the dial target's host.
//
// loader, when non-nil, supplies the CA pool so it is read and parsed once and
// reused across the per-request credentials of a templated upstream; pass nil
// for a fixed-address upstream, in which case the credential loads (and caches)
// its own pool on first use.
func NewClientTLSWithCA(caFile, serverName string, loader *CAPoolLoader) *TLS {
	if loader == nil {
		loader = NewCAPoolLoader(caFile)
	}

	return &TLS{caFile: caFile, serverName: serverName, caLoader: loader}
}

// NewServerTLS returns a [TLS] credential that loads the server certificate and
// private key from certFile and keyFile respectively.
func NewServerTLS(certFile, keyFile string) *TLS {
	return &TLS{
		certFile: certFile,
		keyFile:  keyFile,
	}
}

// NewCAPoolLoader returns a [CAPoolLoader] that reads and parses the CA
// certificate pool from caFile on first use.
func NewCAPoolLoader(caFile string) *CAPoolLoader {
	return &CAPoolLoader{caFile: caFile}
}

// DialOption returns a [grpc.DialOption] that configures the outbound
// connection with TLS. When no custom CA is configured, the server's certificate
// is verified against the system root CA pool. When a custom CA was configured
// via [NewClientTLSWithCA], the server's certificate is verified against that
// custom CA pool instead. The configured server name (when non-empty) is used for
// SNI and verification; otherwise the dial target's host is used. No client
// certificate is presented; use [MTLS] when mutual authentication is required.
func (c *TLS) DialOption() (grpc.DialOption, error) {
	cfg := &tls.Config{
		ServerName: c.serverName,
		MinVersion: minTLSVersion,
	}

	if c.caFile != "" {
		pool, err := c.caLoader.load()
		if err != nil {
			return nil, err
		}

		cfg.RootCAs = pool
	}

	return grpc.WithTransportCredentials(credentials.NewTLS(cfg)), nil
}

// ServerOption returns a [grpc.ServerOption] that configures the server with
// TLS using the certificate and key provided at construction. The server
// requires at least TLS 1.2 and restricts TLS 1.2 sessions to AES-GCM cipher
// suites with ECDHE key exchange.
func (c *TLS) ServerOption() (grpc.ServerOption, error) {
	cert, err := tls.LoadX509KeyPair(c.certFile, c.keyFile)
	if err != nil {
		return nil, fmt.Errorf("failed to load server key pair: %w", err)
	}

	cfg := &tls.Config{
		Certificates: []tls.Certificate{cert},
		MinVersion:   minTLSVersion,
		CipherSuites: preferredCipherSuites,
	}

	return grpc.Creds(credentials.NewTLS(cfg)), nil
}

// Validate reads the configured certificate file and returns a [validation.Errors]
// describing any problems found: an expired or not-yet-valid certificate, a weak
// signature algorithm, or a public key type incompatible with [preferredCipherSuites].
// Client-mode [TLS] credentials (those constructed via [NewClientTLS]) have no
// certFile and will fail with a read error; callers should only invoke Validate
// on server-mode credentials.
func (c *TLS) Validate() error {
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
	)
}

// Encrypted reports whether the transport is encrypted. TLS always encrypts the
// transport, so it returns true.
func (c *TLS) Encrypted() bool {
	return true
}

// load reads and parses the CA pool on the first call, caching the result (or
// the error) for every subsequent call. The returned pool is immutable and safe
// to share across dial options and goroutines.
func (l *CAPoolLoader) load() (*x509.CertPool, error) {
	l.once.Do(func() {
		l.pool, l.err = loadCAPool(l.caFile)
	})

	return l.pool, l.err
}
