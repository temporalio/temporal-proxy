package auth

import (
	"errors"
	"fmt"

	"github.com/temporalio/temporal-proxy/internal/api"
	"github.com/temporalio/temporal-proxy/internal/config"
)

// For returns the Authenticator ac selects: a static token, OIDC/JWKS, or an
// extension server that decides on the proxy's behalf. A nil ac admits every
// request, so authentication stays opt-in; a block that selects none or
// several is a configuration error, and naming an extension server that is
// not configured fails here rather than on the first request.
func For(ac *config.AuthConfig, conns api.Connections) (Authenticator, error) {
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
}
