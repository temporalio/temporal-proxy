package config

import (
	"errors"
	"net/url"

	"github.com/temporalio/temporal-proxy/pkg/validation"
)

type (
	// AuthConfig configures inbound authentication for the proxy listener.
	// Exactly one authenticator must be selected: one of the built-in ones, or
	// an external extension server that decides on the proxy's behalf.
	AuthConfig struct {
		External    *ExternalAuthConfig `yaml:"external"`
		StaticToken *StaticTokenConfig  `yaml:"staticToken"`
		JWKS        *JWKSConfig         `yaml:"jwks"`
	}

	// StaticTokenConfig compares an inbound bearer token against a fixed value.
	StaticTokenConfig struct {
		Token  string `yaml:"token"`
		Header string `yaml:"header"`
		Scheme string `yaml:"scheme"`
	}

	// JWKSConfig verifies an inbound JWT's signature and claims against a JWKS.
	JWKSConfig struct {
		URL       string   `yaml:"url"`
		Audiences []string `yaml:"audiences"`
		Issuer    string   `yaml:"issuer"`
		Header    string   `yaml:"header"`
		Scheme    string   `yaml:"scheme"`
	}

	// ExternalAuthConfig delegates the inbound decision to an extension server
	// implementing api.auth.v1.AuthService, for identity systems the built-in
	// authenticators do not cover.
	//
	// Name selects which configured extension server to ask. CredentialHeaders
	// names the metadata headers carrying the caller's credentials, which the
	// proxy lifts into the request it sends that server and removes from the
	// stream it forwards upstream. It has to be declared because a verdict
	// reports only admit-or-deny, so nothing in the exchange reveals which
	// headers mattered.
	//
	// Leaving it empty does not hide the caller's credentials from the server. The
	// proxy forwards the caller's metadata on the call either way, so the server
	// still sees whatever headers the caller sent; what it loses is the request
	// field naming them, so it has to know which metadata to read and cannot tell
	// a header this proxy vouches for from any other. Nothing is stripped before
	// proxying upstream either, so the caller's credential continues to the
	// upstream alongside any credential configured for it.
	ExternalAuthConfig struct {
		Name              string   `yaml:"name"`
		CredentialHeaders []string `yaml:"credentialHeaders"`
	}

	// CredentialConfig configures the credential the proxy presents to an
	// upstream. Static is the only variant today.
	CredentialConfig struct {
		Static *StaticCredentialConfig `yaml:"static"`
	}

	// StaticCredentialConfig injects a fixed API key as a bearer header on every
	// outbound request to the upstream.
	StaticCredentialConfig struct {
		APIKey string `yaml:"apiKey"`
		Header string `yaml:"header"`
		Scheme string `yaml:"scheme"`
	}
)

// Validate requires exactly one authenticator and checks the selected one.
func (a *AuthConfig) Validate() error {
	n := 0
	if a.External != nil {
		n++
	}
	if a.StaticToken != nil {
		n++
	}
	if a.JWKS != nil {
		n++
	}

	return validation.Validate(
		"",
		func() validation.Errors {
			if n != 1 {
				return validation.Errors{{Message: "exactly one of external, staticToken, or jwks must be set"}}
			}
			return nil
		},
		validation.WhenRules(func() bool { return a.External != nil }, validation.Nested("external", a.External)),
		validation.WhenRules(func() bool { return a.StaticToken != nil }, validation.Nested("staticToken", a.StaticToken)),
		validation.WhenRules(func() bool { return a.JWKS != nil }, validation.Nested("jwks", a.JWKS)),
	)
}

// referentialRules checks that external authentication names a configured
// extension server, given the set of known names. A failure is stamped with the
// referring field's YAML path so it lands on "auth.external"/"name".
//
// The rule is appended at the Config level rather than composed under Validate
// because it needs the full set of extension server names, which is only known
// there. A nil receiver or a blank name yields nothing: the former means no auth
// block at all, and the latter is already reported as required by
// [ExternalAuthConfig.Validate], which leaves this rule no server to name.
func (a *AuthConfig) referentialRules(known map[string]struct{}) []validation.Rule {
	if a == nil || a.External == nil || a.External.Name == "" {
		return nil
	}

	return []validation.Rule{func() validation.Errors {
		if _, ok := known[a.External.Name]; ok {
			return nil
		}

		return validation.Errors{{
			Subject: "auth.external",
			Field:   "name",
			Message: "unknown extension server: " + a.External.Name,
		}}
	}}
}

// Validate requires the token value.
func (c *StaticTokenConfig) Validate() error {
	return validation.Validate(
		"",
		validation.Field("token", c.Token, validation.Required[string]()),
	)
}

// Validate requires a syntactically valid absolute JWKS URL.
func (c *JWKSConfig) Validate() error {
	return validation.Validate(
		"",
		validation.Field("url", c.URL, validation.Required[string](), func(s string) error {
			u, err := url.Parse(s)
			if err != nil || u.Host == "" {
				return errors.New("must be a valid absolute URL")
			}

			if u.Scheme != "https" {
				return errors.New("must use https")
			}

			return nil
		}),
	)
}

// Validate requires the extension server name; the credential header list may be
// empty. Whether a server by that name is actually configured is checked by
// [AuthConfig.referentialRules], which runs where the server list is known.
func (c *ExternalAuthConfig) Validate() error {
	return validation.Validate(
		"",
		validation.Field("name", c.Name, validation.Required[string]()),
	)
}

// Validate requires the static credential and checks it.
func (c *CredentialConfig) Validate() error {
	return validation.Validate(
		"",
		func() validation.Errors {
			if c.Static == nil {
				return validation.Errors{{Field: "static", Message: "is required"}}
			}
			return nil
		},
		validation.WhenRules(func() bool { return c.Static != nil }, validation.Nested("static", c.Static)),
	)
}

// Validate requires the API key.
func (c *StaticCredentialConfig) Validate() error {
	return validation.Validate(
		"",
		validation.Field("apiKey", c.APIKey, validation.Required[string]()),
	)
}
