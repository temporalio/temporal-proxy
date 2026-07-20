package auth

import (
	"context"
	"crypto/subtle"
	"errors"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
)

// StaticTokenAuthenticator authenticates a request by comparing the bearer
// token in its metadata against a fixed configured value.
type StaticTokenAuthenticator struct {
	token  string
	header string
	scheme string
}

// NewStaticTokenAuthenticator builds a StaticTokenAuthenticator. header and
// scheme default to "authorization" and "Bearer" when blank. A blank token is
// an error.
func NewStaticTokenAuthenticator(token, header, scheme string) (*StaticTokenAuthenticator, error) {
	if token == "" {
		return nil, errors.New("auth: static token is required")
	}

	if header == "" {
		header = defaultHeader
	}

	if scheme == "" {
		scheme = defaultScheme
	}

	return &StaticTokenAuthenticator{token: token, header: header, scheme: scheme}, nil
}

// Authenticate compares the extracted token against the configured value in
// constant time.
func (a *StaticTokenAuthenticator) Authenticate(_ context.Context, md metadata.MD) error {
	got, ok := extractToken(md, a.header, a.scheme)
	if !ok {
		return reject(codes.Unauthenticated, "missing or malformed credentials",
			"static token: missing or malformed "+a.header+" header")
	}

	if subtle.ConstantTimeCompare([]byte(got), []byte(a.token)) != 1 {
		return reject(codes.Unauthenticated, "invalid credentials", "static token: value mismatch")
	}

	return nil
}
