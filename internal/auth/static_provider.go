package auth

import (
	"context"
	"errors"
)

// StaticCredentialProvider attaches a fixed bearer header to every outbound
// request to an upstream. It implements google.golang.org/grpc/credentials
// PerRPCCredentials and requires transport security, so gRPC refuses to send
// the credential over an insecure connection.
type StaticCredentialProvider struct {
	header string
	value  string
}

// NewStaticCredentialProvider builds a StaticCredentialProvider. header and
// scheme default to "authorization" and "Bearer" when blank. A blank apiKey is
// an error.
func NewStaticCredentialProvider(apiKey, header, scheme string) (*StaticCredentialProvider, error) {
	if apiKey == "" {
		return nil, errors.New("auth: api key is required")
	}

	if scheme == "" {
		scheme = defaultScheme
	}

	return &StaticCredentialProvider{header: canonicalHeader(header), value: scheme + " " + apiKey}, nil
}

// GetRequestMetadata returns the fixed credential header for every call.
func (p *StaticCredentialProvider) GetRequestMetadata(context.Context, ...string) (map[string]string, error) {
	return map[string]string{p.header: p.value}, nil
}

// Header returns the metadata header this credential sets.
func (p *StaticCredentialProvider) Header() string { return p.header }

// RequireTransportSecurity reports that the credential must only travel over a
// secure transport.
func (p *StaticCredentialProvider) RequireTransportSecurity() bool { return true }
