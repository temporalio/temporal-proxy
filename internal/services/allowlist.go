package services

import (
	"maps"
	"slices"
)

type (
	// Allowlist is the set of services the proxy will forward. One built from no
	// names admits nothing.
	Allowlist interface {
		// Allows reports whether the named service was admitted. It matches a
		// service full name only, so a caller holding a gRPC full method must strip
		// the method first.
		Allows(string) bool

		// ServiceNames returns the admitted names in sorted order, for callers that
		// publish the set rather than query it.
		ServiceNames() []string
	}

	// allowSet is the concrete admission set, keyed by proto full name. It admits
	// the names it was built from plus their compatibility aliases, and does no
	// registry lookup, since configuration validation has already rejected names
	// that cannot be forwarded.
	allowSet map[string]struct{}
)

// NewAllowlist builds an Allowlist admitting names and their compatibility
// aliases, so allowing a service also allows the spellings a client may fall
// back to.
func NewAllowlist(names []string) Allowlist {
	exp := expand(names)
	set := make(allowSet, len(exp))
	for _, name := range exp {
		set[name] = struct{}{}
	}

	return set
}

// Allows reports whether name was admitted.
func (s allowSet) Allows(name string) bool {
	_, ok := s[name]
	return ok
}

// ServiceNames returns every admitted name, aliases included, sorted so the
// order does not shift between runs.
func (s allowSet) ServiceNames() []string {
	return slices.Sorted(maps.Keys(s))
}
