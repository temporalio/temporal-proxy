package auth

import (
	"fmt"

	"github.com/temporalio/temporal-proxy/internal/api"
	"github.com/temporalio/temporal-proxy/internal/config"
)

// For selects the Authenticator ac describes, admitting everything when ac is
// nil. Returns the block's own validation error when it does not set exactly
// one method, and an error when ac names an extension server absent from conns.
func For(ac *config.AuthConfig, conns api.Connections) (Authenticator, error) {
	if ac == nil {
		return AdmitAll(), nil
	}

	if err := ac.Validate(); err != nil {
		return nil, fmt.Errorf("auth: %w", err)
	}

	switch {
	case ac.External != nil:
		cc, ok := conns[ac.External.Name]
		if !ok {
			return nil, fmt.Errorf("auth: external authentication names unknown extension server %q", ac.External.Name)
		}

		return api.NewAuth(cc, ac.External.CredentialHeaders), nil
	case ac.StaticToken != nil:
		return NewStaticTokenAuthenticator(ac.StaticToken.Token, ac.StaticToken.Header, ac.StaticToken.Scheme)
	default:
		return NewJWKSAuthenticator(ac.JWKS.URL, ac.JWKS.Audiences, ac.JWKS.Issuer, ac.JWKS.Header, ac.JWKS.Scheme)
	}
}
