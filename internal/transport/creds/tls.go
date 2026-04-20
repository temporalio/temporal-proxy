package creds

import (
	"crypto/tls"
	"fmt"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

// minTLSVersion is the minimum TLS version accepted on both client and server
// connections. TLS 1.2 is the lowest version still considered secure for
// production workloads.
const minTLSVersion = tls.VersionTLS12

// TLS enables one-way (server-side) TLS on gRPC connections. The server
// presents a certificate to the client; the client is not required to present
// one. Minimum TLS version is 1.2.
type TLS struct {
	certFile string
	keyFile  string
}

// NewTLS returns a [TLS] credential that loads the server certificate and
// private key from certFile and keyFile respectively.
func NewTLS(certFile, keyFile string) *TLS {
	return &TLS{
		certFile: certFile,
		keyFile:  keyFile,
	}
}

// DialOption returns a [grpc.DialOption] that configures the outbound
// connection with TLS, verifying the server's certificate against the system
// root CA pool. No client certificate is presented; use [MTLS] when mutual
// authentication is required.
func (c *TLS) DialOption() (grpc.DialOption, error) {
	return grpc.WithTransportCredentials(credentials.NewClientTLSFromCert(nil, "")), nil
}

// ServerOption returns a [grpc.ServerOption] that configures the server with
// TLS using the certificate and key provided at construction. The server
// requires at least TLS 1.2.
func (c *TLS) ServerOption() (grpc.ServerOption, error) {
	cert, err := tls.LoadX509KeyPair(c.certFile, c.keyFile)
	if err != nil {
		return nil, fmt.Errorf("failed to load server key pair: %w", err)
	}

	cfg := &tls.Config{
		Certificates: []tls.Certificate{cert},
		MinVersion:   minTLSVersion,
	}

	return grpc.Creds(credentials.NewTLS(cfg)), nil
}
