package auth

import (
	"errors"

	"go.uber.org/fx"

	"github.com/temporalio/temporal-proxy/internal/config"
)

// Module provides the inbound Authenticator selected by configuration:
// StaticToken or OIDC/JWKS when exactly one is configured. With no auth block
// it provides a default authenticator that admits every request (authentication
// is opt-in); a block that selects neither or both is a configuration error, so
// an invalid block fails closed rather than admitting traffic. The server adapts
// the Authenticator into a stream interceptor via StreamServerInterceptor.
var Module = fx.Options(fx.Provide(func(cfg *config.Config) (Authenticator, error) {
	auth := cfg.Auth
	if auth == nil {
		return &defaultAuthenticator{}, nil
	}

	switch {
	case auth.StaticToken != nil && auth.JWKS == nil:
		return NewStaticTokenAuthenticator(
			auth.StaticToken.Token,
			auth.StaticToken.Header,
			auth.StaticToken.Scheme,
		)
	case auth.JWKS != nil && auth.StaticToken == nil:
		return NewJWKSAuthenticator(
			auth.JWKS.URL,
			auth.JWKS.Audiences,
			auth.JWKS.Issuer,
			auth.JWKS.Header,
			auth.JWKS.Scheme,
		)
	default:
		return nil, errors.New("auth: exactly one of staticToken or jwks must be configured")
	}
}))
