package config

import (
	"slices"

	"github.com/temporalio/temporal-proxy/pkg/validation"
)

type (
	// CodecServer configures the codec server routes, which decode and encode
	// payloads for clients that reach a Temporal Service without passing through
	// the gateway. They are served by the shared HTTP listener, which owns the
	// address and TLS.
	CodecServer struct {
		Enabled bool        `yaml:"enabled"`
		CORS    CORSConfig  `yaml:"cors"`
		Auth    *AuthConfig `yaml:"auth"`
	}

	// CORSConfig controls the cross-origin headers the codec server answers with,
	// which a browser-based caller such as the Temporal Cloud UI requires.
	CORSConfig struct {
		Origins     []string `yaml:"origins"`
		Credentials bool     `yaml:"credentials"`
	}
)

// Validate checks the codec server's CORS and authentication. A disabled block
// is not checked at all, so a half-written one can be left in place. CORS
// credentials require explicit origins, and a "*" origin is rejected. Whether
// auth is required depends on the listener, so [HTTP.Validate] checks that.
func (c *CodecServer) Validate() error {
	if !c.Enabled {
		return nil
	}

	return validation.Validate(
		"",
		validation.WhenRules(
			func() bool { return c.CORS.Credentials && len(c.CORS.Origins) == 0 },
			func() validation.Errors {
				return validation.Errors{{
					Subject: "cors",
					Field:   "origins",
					Message: "origins is required when credentials is enabled",
				}}
			},
		),
		validation.WhenRules(
			func() bool { return slices.Contains(c.CORS.Origins, "*") },
			func() validation.Errors {
				return validation.Errors{{
					Subject: "cors",
					Field:   "origins",
					Message: `must not contain "*"; list the real origins allowed to reach this codec server`,
				}}
			},
		),
		validation.WhenNested(func() bool { return c.Auth != nil }, "auth", c.Auth),
	)
}

// enabled reports whether the codec server routes are served.
func (c *CodecServer) enabled() bool { return c.Enabled }

// authConfig returns the codec server's caller authentication, or nil.
func (c *CodecServer) authConfig() *AuthConfig { return c.Auth }
