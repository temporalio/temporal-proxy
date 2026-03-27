package server

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

type (
	Credentials interface {
		Server() (grpc.ServerOption, error)
	}

	InsecureCredentials struct{}

	TLSCredentials struct {
		certFile string
		keyFile  string
	}

	MTLSCredentials struct {
		caFile   string
		certFile string
		keyFile  string
	}
)

func NewInsecureCredentials() *InsecureCredentials {
	return new(InsecureCredentials)
}

func NewTLSCredentials(certFile, keyFile string) *TLSCredentials {
	return &TLSCredentials{
		certFile: certFile,
		keyFile:  keyFile,
	}
}

func NewMTLSCredentials(caFile, certFile, keyFile string) *MTLSCredentials {
	return &MTLSCredentials{
		caFile:   caFile,
		certFile: certFile,
		keyFile:  keyFile,
	}
}

func (i *InsecureCredentials) Server() (grpc.ServerOption, error) {
	return grpc.Creds(insecure.NewCredentials()), nil
}

func (c *TLSCredentials) Server() (grpc.ServerOption, error) {
	creds, err := credentials.NewServerTLSFromFile(c.certFile, c.keyFile)
	if err != nil {
		return nil, fmt.Errorf("failed to create TLS credentials: %w", err)
	}

	return grpc.Creds(creds), nil
}

func (c *MTLSCredentials) Server() (grpc.ServerOption, error) {
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
		ClientAuth:   tls.RequireAndVerifyClientCert,
		ClientCAs:    ca,
		Certificates: []tls.Certificate{cert},
		MinVersion:   tls.VersionTLS12,
	})), nil
}
