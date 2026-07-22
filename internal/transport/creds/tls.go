package creds

import (
	"crypto/tls"
	"fmt"

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

// TLS enables one-way (server-side) TLS on gRPC connections. The server
// presents a certificate to the client; the client is not required to present
// one. Minimum TLS version is 1.2.
type TLS struct {
	certFile   string
	keyFile    string
	serverName string
}

// NewClientTLS returns a [TLS] credential suitable for outbound (client-side)
// connections. The server's certificate is verified against the system root CA
// pool; no client certificate is presented. Use [NewMTLS] when mutual
// authentication is required. serverName overrides the name used for SNI and
// certificate verification; when empty it defaults to the dial target's host.
func NewClientTLS(serverName string) *TLS {
	return &TLS{serverName: serverName}
}

// NewServerTLS returns a [TLS] credential that loads the server certificate and
// private key from certFile and keyFile respectively.
func NewServerTLS(certFile, keyFile string) *TLS {
	return &TLS{
		certFile: certFile,
		keyFile:  keyFile,
	}
}

// DialOption returns a [grpc.DialOption] that configures the outbound
// connection with TLS, verifying the server's certificate against the system
// root CA pool. The configured server name (when non-empty) is used for SNI and
// verification; otherwise the dial target's host is used. No client certificate
// is presented; use [MTLS] when mutual authentication is required.
func (c *TLS) DialOption() (grpc.DialOption, error) {
	return grpc.WithTransportCredentials(credentials.NewClientTLSFromCert(nil, c.serverName)), nil
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
