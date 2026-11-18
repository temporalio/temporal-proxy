package config

import (
	"net"

	"go.temporal.io/proxy/pkg/validation"
)

type (
	// HTTP configures the proxy's HTTP listeners. Listen is inline, so the shared
	// listener's address and TLS are the block's own hostPort, insecure, and tls
	// keys; it serves the codec server and pprof route groups, each carrying its
	// own CORS and auth, since their callers differ. StandaloneNexus is a
	// listener of its own (see [StandaloneNexus]).
	HTTP struct {
		Listen          ListenConfig    `yaml:",inline"`
		CodecServer     CodecServer     `yaml:"codecServer"`
		PProf           PProf           `yaml:"pprof"`
		StandaloneNexus StandaloneNexus `yaml:"standaloneNexus"`
	}

	// routeGroup is a set of routes served by an HTTP listener. Each group
	// decides whether it is on and how its callers authenticate.
	routeGroup interface {
		Validate() error
		enabled() bool
		authConfig() *AuthConfig
	}

	// namedGroup pairs a route group with its key under its listener's block.
	namedGroup struct {
		name  string
		group routeGroup
	}
)

// Validate checks the shared listener and every enabled route group, then the
// Standalone Nexus listener. With no group enabled a listener binds nothing, so
// nothing is checked for it. A group reachable beyond loopback requires
// authentication, and its listener requires TLS once any such group has it,
// because a browser will not send a token over plaintext. The two listeners
// must not share an address.
func (h *HTTP) Validate() error {
	rules := append(groupRules(&h.Listen, h.groups()),
		validation.Nested("standaloneNexus", &h.StandaloneNexus),
		h.distinctListenersRule(),
	)

	return validation.Validate("", rules...)
}

// groups lists every route group the shared listener can serve. A new group is
// a field on HTTP plus an entry here.
func (h *HTTP) groups() []namedGroup {
	return []namedGroup{
		{name: "codecServer", group: &h.CodecServer},
		{name: "pprof", group: &h.PProf},
	}
}

// referentialRules checks that every enabled group's external auth, on either
// listener, names a configured extension server.
func (h *HTTP) referentialRules(known map[string]struct{}) []validation.Rule {
	var rules []validation.Rule
	for _, g := range h.groups() {
		if g.group.enabled() {
			rules = append(rules, g.group.authConfig().referentialRules("http."+g.name+".auth.external", known)...)
		}
	}

	for _, g := range h.StandaloneNexus.groups() {
		rules = append(rules,
			g.group.authConfig().referentialRules("http.standaloneNexus."+g.name+".auth.external", known)...)
	}

	return rules
}

// distinctListenersRule refuses a Standalone Nexus listener bound to the shared
// listener's address while both serve something. A port-zero address picks a
// free port each time, so two of them never collide.
func (h *HTTP) distinctListenersRule() validation.Rule {
	return validation.WhenRules(
		func() bool {
			hp := h.StandaloneNexus.Listen.HostPort
			_, port, err := net.SplitHostPort(hp)

			return err == nil && port != "0" && hp == h.Listen.HostPort &&
				h.sharedEnabled() && h.StandaloneNexus.enabled()
		},
		func() validation.Errors {
			return validation.Errors{{
				Subject: "standaloneNexus",
				Field:   "hostPort",
				Message: "must differ from http.hostPort; Standalone Nexus has a listener of its own",
			}}
		},
	)
}

// sharedEnabled reports whether any route group on the shared listener is on.
func (h *HTTP) sharedEnabled() bool {
	for _, g := range h.groups() {
		if g.group.enabled() {
			return true
		}
	}

	return false
}

// groupRules returns the listener rules for the enabled groups among groups,
// or none when no group is enabled.
func groupRules(listen *ListenConfig, groups []namedGroup) []validation.Rule {
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

	return rules
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
