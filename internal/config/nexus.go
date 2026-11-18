package config

import (
	"errors"
	"fmt"
	"path"
	"slices"
	"strings"

	"go.temporal.io/proxy/internal/services"
	"go.temporal.io/proxy/pkg/validation"
)

// StandaloneNexus serves the Nexus HTTP connector on a listener of its own,
// apart from the shared HTTP listener, so it can be exposed, firewalled, and
// given its own TLS and client-certificate policy without the codec server and
// pprof routes. Listen is inline, so its address and TLS are the block's own
// hostPort, insecure, and tls keys. Each connector is mounted under its own
// pathPrefix on that listener.
type StandaloneNexus struct {
	Listen     ListenConfig     `yaml:",inline"`
	Connectors []NexusConnector `yaml:"connectors"`
}

// NexusConnector is one connector on the Standalone Nexus listener. It serves
// the connector's routes under PathPrefix and starts every operation in
// Namespace, which is routed and translated like any namespace. Its callers may
// start operations on any Nexus Endpoint, and their payloads use the
// namespace's codec like every other request.
//
// Identity is the default identity for start, cancel, and terminate requests
// whose body sets none; a caller's own identity wins and is not verified.
// MaxRequestBodySize caps a request body. Both fall back to the connector's
// defaults when unset. Auth authenticates the connector's callers, and is
// required once the listener is reachable beyond loopback.
type NexusConnector struct {
	Name               string      `yaml:"name"`
	PathPrefix         string      `yaml:"pathPrefix"`
	Namespace          string      `yaml:"namespace"`
	Identity           string      `yaml:"identity"`
	MaxRequestBodySize ByteSize    `yaml:"maxRequestBodySize"`
	Auth               *AuthConfig `yaml:"auth"`
}

// Validate checks the listener once any connector is listed, then each
// connector and the rules spanning them. A connector reachable beyond loopback
// requires authentication, and the listener requires TLS once any connector has
// it.
func (s *StandaloneNexus) Validate() error {
	rules := append(groupRules(&s.Listen, s.groups()), s.connectorRules()...)

	return validation.Validate("", rules...)
}

// enabled reports whether the listener serves anything.
func (s *StandaloneNexus) enabled() bool { return len(s.Connectors) > 0 }

// groups lists the connectors as the listener's route groups.
func (s *StandaloneNexus) groups() []namedGroup {
	groups := make([]namedGroup, len(s.Connectors))
	for i := range s.Connectors {
		groups[i] = namedGroup{name: fmt.Sprintf("connectors[%d]", i), group: &s.Connectors[i]}
	}

	return groups
}

// connectorRules checks what spans the connectors: unique names and path
// prefixes, and no connector's path prefix nested in another's.
func (s *StandaloneNexus) connectorRules() []validation.Rule {
	names := make([]string, len(s.Connectors))
	prefixes := make([]string, len(s.Connectors))
	for i := range s.Connectors {
		names[i] = s.Connectors[i].Name
		prefixes[i] = s.Connectors[i].PathPrefix
	}

	rules := []validation.Rule{
		validation.Field("connectors[name]", names, validation.Unique[string]()),
		validation.Field("connectors[pathPrefix]", prefixes, validation.Unique[string]()),
	}

	for i := range s.Connectors {
		subject := fmt.Sprintf("connectors[%d]", i)
		rules = append(rules, func() validation.Errors {
			var errs validation.Errors
			for j := range i {
				if prefixes[i] != prefixes[j] && pathsNest(prefixes[i], prefixes[j]) {
					errs = append(errs, validation.Error{
						Subject: subject,
						Field:   "pathPrefix",
						Message: fmt.Sprintf("overlaps connectors[%d] pathPrefix %q", j, prefixes[j]),
					})
				}
			}

			return errs
		})
	}

	return rules
}

// Validate checks the connector's own fields. Rules that span connectors, or
// need routing or extension servers, run where those are known. Whether auth is
// required depends on the listener, so [StandaloneNexus.Validate] checks that.
func (n *NexusConnector) Validate() error {
	return validation.Validate(
		"",
		validation.Field("name", n.Name, validation.Required[string]()),
		validation.Field("pathPrefix", n.PathPrefix, validation.Required[string](), checkPathPrefix),
		validation.Field("namespace", n.Namespace, validation.Required[string]()),
		validation.Field("maxRequestBodySize", n.MaxRequestBodySize, validation.GTE[ByteSize](0)),
		validation.WhenNested(func() bool { return n.Auth != nil }, "auth", n.Auth),
	)
}

// enabled reports that the connector is served: a listed connector is always on.
func (n *NexusConnector) enabled() bool { return true }

// authConfig returns the connector's caller authentication, or nil.
func (n *NexusConnector) authConfig() *AuthConfig { return n.Auth }

// nexusReferentialRules checks that every connector's namespace routes to an
// upstream.
func (h *HTTP) nexusReferentialRules(routing *Routing) []validation.Rule {
	var rules []validation.Rule
	for i := range h.StandaloneNexus.Connectors {
		ns := h.StandaloneNexus.Connectors[i].Namespace
		if ns == "" {
			continue
		}

		rules = append(rules, func() validation.Errors {
			if routing.namespaceUpstream(ns) != "" {
				return nil
			}

			return validation.Errors{{
				Subject: fmt.Sprintf("http.standaloneNexus.connectors[%d]", i),
				Field:   "namespace",
				Message: fmt.Sprintf(
					"%q routes to no upstream; add a routing rule matching it on namespace alone, or set routing.default",
					ns),
			}}
		})
	}

	return rules
}

// nexusServiceRule refuses Nexus connectors when allowed, the services the
// proxy forwards, leaves out WorkflowService, which every connector call uses.
func (h *HTTP) nexusServiceRule(allowed []string) validation.Rule {
	return validation.WhenRules(
		func() bool { return h.StandaloneNexus.enabled() && !slices.Contains(allowed, services.WorkflowService) },
		func() validation.Errors {
			return validation.Errors{{
				Subject: "http",
				Field:   "standaloneNexus",
				Message: "requires allowedServices to include " + services.WorkflowService,
			}}
		},
	)
}

// namespaceUpstream returns the upstream that serves a request naming ns and
// carrying no metadata, or "" when none does. It mirrors the router: rules in
// order, first match wins, then the default upstream. A rule that matches on
// metadata never matches, and the system upstream serves only requests with no
// namespace, so neither can select one here.
func (r *Routing) namespaceUpstream(ns string) string {
	for i := range r.Rules {
		m := &r.Rules[i].Match
		if len(m.Metadata) > 0 {
			continue
		}

		// A pattern that does not compile is reported by RoutingMatch.Validate.
		compiled, errs := m.compile()
		if len(errs) == 0 && compiled.ns.Match(ns) {
			return r.Rules[i].Upstream
		}
	}

	return r.DefaultUpstream
}

// checkPathPrefix requires an absolute, clean, literal path other than the
// root, since a connector's routes are served beneath its prefix. The listener
// serves nothing but connectors, so no other route group constrains it.
func checkPathPrefix(p string) error {
	switch {
	case !strings.HasPrefix(p, "/"):
		return errors.New("must be an absolute path starting with /")
	case p == "/":
		return errors.New("must not be /; a connector's routes are served beneath its pathPrefix")
	case path.Clean(p) != p:
		return errors.New("must be a clean path with no trailing slash, empty segment, or . or .. segment")
	case strings.ContainsAny(p, "{}?#% \t\r\n"):
		return errors.New("must be a literal path with no wildcard, query, fragment, escape, or whitespace")
	}

	return nil
}

// pathsNest reports whether one of two clean absolute paths is the other or
// lies beneath it, segment by segment. An empty path nests with nothing.
func pathsNest(a, b string) bool {
	if a == "" || b == "" {
		return false
	}

	return a == b || strings.HasPrefix(a, b+"/") || strings.HasPrefix(b, a+"/")
}
