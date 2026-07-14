package router

import "slices"

type (
	// Mux selects the upstream that serves a request by matching it against an
	// ordered list of rules. It holds upstream names only, not connections, so
	// callers map the name Switch returns to a connection. A Mux is read-only
	// after construction and safe for concurrent use.
	Mux struct {
		def   string
		sys   string
		rules []Rule
	}

	// Matcher reports whether a string satisfies some pattern. Rules use it to
	// match namespaces and metadata values, keeping Mux decoupled from any
	// particular matching implementation.
	Matcher interface {
		Match(string) bool
	}

	// Rule routes every request it matches to a named upstream. A request
	// matches when the rule's namespace matcher accepts the request namespace
	// and, for every constrained metadata key, at least one of the request's
	// values for that key is accepted. Metadata keys are compared as stored, so
	// the rule builder is responsible for canonicalizing them (gRPC lowercases
	// metadata keys). Construct rules within this package.
	Rule struct {
		upstream string
		ns       Matcher
		meta     map[string]Matcher
	}
)

// New returns a Mux that evaluates rules in order. defUpstream is returned when
// no rule matches; sysUpstream, when non-empty, serves a request that carries
// no namespace and matches no rule. Either name may be empty, in which case
// Switch can return "" to signal that the request is unroutable.
func New(defUpstream, sysUpstream string, rules ...Rule) *Mux {
	return &Mux{
		def:   defUpstream,
		sys:   sysUpstream,
		rules: slices.Clone(rules),
	}
}

// Switch returns the name of the upstream that serves a request with the given
// namespace and metadata. Rules are evaluated in order and the first match
// wins. When no rule matches, a request with no namespace goes to the system
// upstream if one is configured, and every other request goes to the default
// upstream. The result is empty when the selected upstream is unset, which the
// caller reports as an unroutable request.
func (m *Mux) Switch(ns string, md map[string][]string) string {
	for _, rule := range m.rules {
		if rule.matches(ns, md) {
			return rule.upstream
		}
	}

	if ns == "" && m.sys != "" {
		return m.sys
	}

	return m.def
}

// matches reports whether the rule accepts a request with the given namespace
// and metadata: the namespace must match and, for every metadata key the rule
// constrains, at least one of the request's values for that key must match.
func (r *Rule) matches(ns string, md map[string][]string) bool {
	if r.ns == nil || !r.ns.Match(ns) {
		return false
	}

	for key, matcher := range r.meta {
		if matcher == nil || !slices.ContainsFunc(md[key], matcher.Match) {
			return false
		}
	}

	return true
}
