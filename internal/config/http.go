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

	// routeGroup is a set of routes served by the shared HTTP listener. Each
	// group decides whether it is on and how its callers authenticate.
	routeGroup interface {
		Validate() error
		enabled() bool
		authConfig() *AuthConfig
	}

	// namedGroup pairs a route group with its key under http.
	namedGroup struct {
		name  string
		group routeGroup
	}
)

// Validate checks the listener and every enabled route group. With no group
// enabled nothing binds, so nothing is checked. A group reachable beyond
// loopback requires authentication, and the listener requires TLS once any
// such group has it, because a browser will not send a token over plaintext.
func (h *HTTP) Validate() error {
	return validateGroups(&h.Listen, h.groups())
}

// groups lists every route group the listener can serve. A new group is a
// field on HTTP plus an entry here.
func (h *HTTP) groups() []namedGroup {
	return []namedGroup{{name: "codecServer", group: &h.CodecServer}}
}

// referentialRules checks that every enabled group's external auth names a
// configured extension server.
func (h *HTTP) referentialRules(known map[string]struct{}) []validation.Rule {
	var rules []validation.Rule
	for _, g := range h.groups() {
		if g.group.enabled() {
			rules = append(rules, g.group.authConfig().referentialRules("http."+g.name+".auth.external", known)...)
		}
	}

	return rules
}

// validateGroups applies the listener rules for the enabled groups among
// groups.
func validateGroups(listen *ListenConfig, groups []namedGroup) error {
	var enabled []namedGroup
	authed := false
	for _, g := range groups {
		if g.group.enabled() {
			enabled = append(enabled, g)
			authed = authed || g.group.authConfig() != nil
		}
	}

	if len(enabled) == 0 {
		return nil
	}

	loopback := isLoopback(listen.HostPort)
	rules := []validation.Rule{
		validation.Field("hostPort", listen.HostPort, validation.Required[string](), validation.IsHostPort()),
	}

	for _, g := range enabled {
		rules = append(rules, validation.WhenRules(
			func() bool { return !loopback && g.group.authConfig() == nil },
			func() validation.Errors {
				return validation.Errors{{
					Subject: g.name,
					Field:   "auth",
					Message: "auth is required unless hostPort is loopback",
				}}
			},
		))
	}

	rules = append(rules, validation.WhenRules(
		func() bool { return !loopback && authed && listen.TLS == nil },
		func() validation.Errors {
			return validation.Errors{{
				Field:   "tls",
				Message: "tls is required when auth is configured and hostPort is not loopback",
			}}
		},
	))

	for _, g := range enabled {
		rules = append(rules, validation.Nested(g.name, g.group))
	}

	rules = append(rules,
		validation.WhenNested(func() bool { return listen.TLS != nil }, "tls", listen.TLS),
		listen.insecureRule(),
	)

	return validation.Validate("", rules...)
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
