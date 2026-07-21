package config

import (
	"errors"
	"net/url"

	"github.com/temporalio/temporal-proxy/pkg/validation"
)

type (
	// AuthConfig configures inbound authentication for the proxy listener.
	// Exactly one authenticator must be selected.
	AuthConfig struct {
		StaticToken *StaticTokenConfig `yaml:"staticToken"`
		JWKS        *JWKSConfig        `yaml:"jwks"`
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
				return validation.Errors{{Message: "exactly one of staticToken or jwks must be set"}}
			}
			return nil
		},
		validation.WhenRules(func() bool { return a.StaticToken != nil }, validation.Nested("staticToken", a.StaticToken)),
		validation.WhenRules(func() bool { return a.JWKS != nil }, validation.Nested("jwks", a.JWKS)),
	)
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
