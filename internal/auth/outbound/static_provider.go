package outbound

import (
	"context"
	"errors"
	"strings"

	"go.temporal.io/proxy/internal/config"
)

const defaultHeader = "authorization"

// StaticCredentialProvider attaches a fixed "<scheme> <apiKey>" header (or the
// bare apiKey when the scheme is config.NoScheme) to every outbound request on
// the connection it is dialed with. It implements
// google.golang.org/grpc/credentials PerRPCCredentials and requires transport
// security, so gRPC refuses to send the credential over an insecure connection.
type StaticCredentialProvider struct {
	header string
	value  string
}

// NewStaticCredentialProvider builds a StaticCredentialProvider. header
// defaults to "authorization" when blank, and scheme is resolved by
// config.ResolveScheme. A blank apiKey is an error.
func NewStaticCredentialProvider(apiKey, header, scheme string) (*StaticCredentialProvider, error) {
	if apiKey == "" {
		return nil, errors.New("auth: api key is required")
	}

	value := apiKey
	if scheme = config.ResolveScheme(scheme); scheme != "" {
		value = scheme + " " + apiKey
	}

	return &StaticCredentialProvider{header: canonicalHeader(header), value: value}, nil
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

// canonicalHeader returns the metadata header this credential will occupy: the
// default when h is blank, otherwise h lowercased. gRPC canonicalizes metadata
// keys to lowercase, so normalizing here keeps a mixed-case configured header
// matching what the credential sends and what DialOptions strips.
func canonicalHeader(h string) string {
	if h == "" {
		return defaultHeader
	}

	return strings.ToLower(h)
}
