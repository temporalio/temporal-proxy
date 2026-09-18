package config

import (
	"net"
	"slices"

	"github.com/temporalio/temporal-proxy/pkg/validation"
)

type (
	// CodecServer configures the HTTP codec server, which decodes and encodes
	// payloads for clients that reach a Temporal Service without passing through
	// the gateway. Listen is inline, so its address and TLS are the block's own
	// hostPort, insecure, and tls keys.
	CodecServer struct {
		Enabled bool         `yaml:"enabled"`
		Listen  ListenConfig `yaml:",inline"`
		CORS    CORSConfig   `yaml:"cors"`
		Auth    *AuthConfig  `yaml:"auth"`
	}

	// CORSConfig controls the cross-origin headers the codec server answers with,
	// which a browser-based caller such as the Temporal Cloud UI requires.
	CORSConfig struct {
		Origins     []string `yaml:"origins"`
		Credentials bool     `yaml:"credentials"`
	}
)

// Validate checks the codec server's address, transport, and authentication. A
// disabled block is not checked at all, so a half-written one can be left in
// place. An enabled one reachable beyond loopback requires authentication, and
// requires TLS once it has any, because a browser will not send a token over
// plaintext.
func (c *CodecServer) Validate() error {
	if !c.Enabled {
		return nil
	}

	loopback := isLoopback(c.Listen.HostPort)

	return validation.Validate(
		"",
		validation.Field("hostPort", c.Listen.HostPort, validation.Required[string](), validation.IsHostPort()),
		validation.WhenRules(
			func() bool { return !loopback && c.Auth == nil },
			func() validation.Errors {
				return validation.Errors{{
					Field:   "auth",
					Message: "auth is required unless hostPort is loopback",
				}}
			},
		),
		validation.WhenRules(
			func() bool { return !loopback && c.Auth != nil && c.Listen.TLS == nil },
			func() validation.Errors {
				return validation.Errors{{
					Field:   "tls",
					Message: "tls is required when auth is configured and hostPort is not loopback",
				}}
			},
		),
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
		validation.WhenNested(func() bool { return c.Listen.TLS != nil }, "tls", c.Listen.TLS),
		c.Listen.insecureRule(),
	)
}

// isLoopback reports whether hostPort binds only the loopback interface. An
// empty host binds every interface, so it is not loopback however the port
// reads.
func isLoopback(hostPort string) bool {
	host, _, err := net.SplitHostPort(hostPort)
	if err != nil {
		return false
	}

	switch host {
	case "":
		return false
	case "localhost":
		return true
	}

	ip := net.ParseIP(host)

	return ip != nil && ip.IsLoopback()
}
