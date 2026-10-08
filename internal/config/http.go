package config

import (
	"net"

	"github.com/temporalio/temporal-proxy/pkg/validation"
)

type (
	// HTTP configures the shared HTTP listener and the route groups it serves.
	// Listen is inline, so its address and TLS are the block's own hostPort,
	// insecure, and tls keys. Each group carries its own CORS and auth, since
	// their callers differ.
	HTTP struct {
		Listen      ListenConfig `yaml:",inline"`
		CodecServer CodecServer  `yaml:"codecServer"`
	}
)

// Validate checks the listener and every enabled route group. With no group
// enabled nothing binds, so nothing is checked. A group reachable beyond
// loopback requires authentication, and the listener requires TLS once such a
// group has any, because a browser will not send a token over plaintext.
func (h *HTTP) Validate() error {
	if !h.CodecServer.Enabled {
		return nil
	}

	loopback := isLoopback(h.Listen.HostPort)

	return validation.Validate(
		"",
		validation.Field("hostPort", h.Listen.HostPort, validation.Required[string](), validation.IsHostPort()),
		validation.WhenRules(
			func() bool { return !loopback && h.CodecServer.Auth == nil },
			func() validation.Errors {
				return validation.Errors{{
					Subject: "codecServer",
					Field:   "auth",
					Message: "auth is required unless hostPort is loopback",
				}}
			},
		),
		validation.WhenRules(
			func() bool { return !loopback && h.CodecServer.Auth != nil && h.Listen.TLS == nil },
			func() validation.Errors {
				return validation.Errors{{
					Field:   "tls",
					Message: "tls is required when auth is configured and hostPort is not loopback",
				}}
			},
		),
		validation.Nested("codecServer", &h.CodecServer),
		validation.WhenNested(func() bool { return h.Listen.TLS != nil }, "tls", h.Listen.TLS),
		h.Listen.insecureRule(),
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
