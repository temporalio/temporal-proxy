package auth

import (
	"errors"
	"fmt"

	"go.uber.org/fx"

	"github.com/temporalio/temporal-proxy/internal/api"
	"github.com/temporalio/temporal-proxy/internal/config"
)

// Module provides the inbound Authenticator selected by configuration: a static
// token, OIDC/JWKS, or an extension server that decides on the proxy's behalf,
// when exactly one is configured. With no auth block it provides a default
// authenticator that admits every request (authentication is opt-in); a block
// that selects none or several is a configuration error, so an invalid block
// fails closed rather than admitting traffic. The server adapts the Authenticator
// into a stream interceptor via StreamServerInterceptor.
//
// The external variant needs a connection to the extension server that answers
// for it, which is why the selection depends on the configured extension server
// connections. Naming one that is not configured fails here rather than on the
// first request, since an authenticator with nowhere to ask would reject every
// caller.
var Module = fx.Options(fx.Provide(func(cfg *config.Config, conns api.Connections) (Authenticator, error) {
	ac := cfg.Auth
	if ac == nil {
		return AdmitAll(), nil
	}

	selected := 0
	for _, set := range []bool{ac.External != nil, ac.StaticToken != nil, ac.JWKS != nil} {
		if set {
			selected++
		}
	}

	if selected != 1 {
		return nil, errors.New("auth: exactly one of external, staticToken, or jwks must be configured")
	}

	switch {
	case ac.External != nil:
		cc, ok := conns[ac.External.Name]
		if !ok {
			return nil, fmt.Errorf("auth: external authentication names unknown extension server %q", ac.External.Name)
		}

		return api.NewAuth(cc, ac.External.CredentialHeaders), nil
	case ac.StaticToken != nil:
		return NewStaticTokenAuthenticator(
			ac.StaticToken.Token,
			ac.StaticToken.Header,
			ac.StaticToken.Scheme,
		)
	default:
		return NewJWKSAuthenticator(
			ac.JWKS.URL,
			ac.JWKS.Audiences,
			ac.JWKS.Issuer,
			ac.JWKS.Header,
			ac.JWKS.Scheme,
		)
	}
}))
