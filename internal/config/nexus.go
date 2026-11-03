package config

import (
	"errors"
	"fmt"
	"path"
	"slices"
	"strings"

	"go.temporal.io/proxy/internal/services"
	"go.temporal.io/proxy/internal/transport/meta"
	"go.temporal.io/proxy/pkg/validation"
)

// NexusConnector is one block of the Nexus HTTP connector on the shared HTTP
// listener. It serves the connector's routes under PathPrefix and starts
// every operation in Namespace, which is routed and translated like any
// namespace. Its callers may start operations on any Nexus Endpoint, and
// their payloads use the namespace's codec like every other request.
//
// Identity is the default identity for start, cancel, and terminate requests
// whose body sets none; a caller's own identity wins and is not verified.
// MaxRequestBodySize caps a request body. Both fall back to the connector's
// defaults when unset. Auth authenticates the block's callers, and is required
// once the listener is reachable beyond loopback.
type NexusConnector struct {
	Name               string      `yaml:"name"`
	PathPrefix         string      `yaml:"pathPrefix"`
	Namespace          string      `yaml:"namespace"`
	Identity           string      `yaml:"identity"`
	MaxRequestBodySize ByteSize    `yaml:"maxRequestBodySize"`
	Auth               *AuthConfig `yaml:"auth"`
}

// Validate checks the block's own fields. Rules that span blocks, or need
// routing or extension servers, run where those are known. Whether auth is
// required depends on the listener, so [HTTP.Validate] checks that.
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

// enabled reports that the block is served: a listed block is always on.
func (n *NexusConnector) enabled() bool { return true }

// authConfig returns the block's caller authentication, or nil.
func (n *NexusConnector) authConfig() *AuthConfig { return n.Auth }

// nexusRules checks what spans the blocks: unique names and path prefixes, and
// no block's path prefix nested in another's.
func (h *HTTP) nexusRules() []validation.Rule {
	names := make([]string, len(h.StandaloneNexus))
	prefixes := make([]string, len(h.StandaloneNexus))
	for i := range h.StandaloneNexus {
		names[i] = h.StandaloneNexus[i].Name
		prefixes[i] = h.StandaloneNexus[i].PathPrefix
	}

	rules := []validation.Rule{
		validation.Field("standaloneNexus[name]", names, validation.Unique[string]()),
		validation.Field("standaloneNexus[pathPrefix]", prefixes, validation.Unique[string]()),
	}

	for i := range h.StandaloneNexus {
		subject := fmt.Sprintf("standaloneNexus[%d]", i)
		rules = append(rules, func() validation.Errors {
			var errs validation.Errors
			for j := range i {
				if prefixes[i] != prefixes[j] && pathsNest(prefixes[i], prefixes[j]) {
					errs = append(errs, validation.Error{
						Subject: subject,
						Field:   "pathPrefix",
						Message: fmt.Sprintf("overlaps standaloneNexus[%d] pathPrefix %q", j, prefixes[j]),
					})
				}
			}

			return errs
		})
	}

	return rules
}

// nexusReferentialRules checks that every block's namespace routes to an
// upstream.
func (h *HTTP) nexusReferentialRules(routing *Routing) []validation.Rule {
	var rules []validation.Rule
	for i := range h.StandaloneNexus {
		ns := h.StandaloneNexus[i].Namespace
		if ns == "" {
			continue
		}

		rules = append(rules, func() validation.Errors {
			if routing.namespaceUpstream(ns) != "" {
				return nil
			}

			return validation.Errors{{
				Subject: fmt.Sprintf("http.standaloneNexus[%d]", i),
				Field:   "namespace",
				Message: fmt.Sprintf(
					"%q routes to no upstream; add a routing rule matching it on namespace alone, or set routing.default",
					ns),
			}}
		})
	}

	return rules
}

// nexusServiceRule refuses Nexus blocks when allowed, the services the proxy
// forwards, leaves out WorkflowService, which every connector call uses.
func (h *HTTP) nexusServiceRule(allowed []string) validation.Rule {
	return validation.WhenRules(
		func() bool { return len(h.StandaloneNexus) > 0 && !slices.Contains(allowed, services.WorkflowService) },
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

// checkPathPrefix requires an absolute, clean, literal path of at least two
// segments that leaves the other route groups' routes alone. The codec server
// mounts at the root and serves /{namespace}/encode and its siblings, so a
// one-segment prefix would take a namespace's routes from it, and a
// two-segment prefix ending in a codec server route would take that route (a
// block mounted at /a/decode registers both /a/decode and /a/decode/, so it
// would take /a/decode from the codec server).
func checkPathPrefix(p string) error {
	switch {
	case !strings.HasPrefix(p, "/"):
		return errors.New("must be an absolute path starting with /")
	case p == "/":
		return errors.New("must not be /; the codec server is served at the root")
	case path.Clean(p) != p:
		return errors.New("must be a clean path with no trailing slash, empty segment, or . or .. segment")
	case strings.ContainsAny(p, "{}?#% \t\r\n"):
		return errors.New("must be a literal path with no wildcard, query, fragment, escape, or whitespace")
	}

	segments := strings.Split(p[1:], "/")
	if len(segments) == 1 {
		return errors.New("must have at least two segments; one would shadow the codec server's /{namespace}/ routes")
	}

	if last := segments[len(segments)-1]; len(segments) == 2 && slices.Contains(meta.CodecServerRoutes(), "/"+last) {
		return fmt.Errorf("would shadow the codec server's /{namespace}/%s route", last)
	}

	if pathsNest(p, strings.TrimSuffix(meta.PProfRoute, "/")) {
		return errors.New("overlaps the pprof routes under " + meta.PProfRoute)
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
