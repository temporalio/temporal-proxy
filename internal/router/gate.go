package router

import (
	"strings"

	"github.com/temporalio/temporal-proxy/internal/services"
)

type (
	// Gate reports whether the proxy exposes a gRPC service. Admission is separate
	// from routing: the gate decides whether the proxy answers for a service at
	// all, while the Director decides which upstream serves an admitted request.
	Gate interface {
		Allows(service string) bool
	}

	// serviceSet is the Gate backed by a fixed set of service names.
	serviceSet map[string]struct{}
)

// NewGate returns a Gate admitting names plus each name's compatibility alias
// (see services.Expand), so a caller names the service it means and does not
// have to track alias spellings itself.
func NewGate(names []string) Gate {
	expanded := services.Expand(names)
	set := make(serviceSet, len(expanded))
	for _, name := range expanded {
		set[name] = struct{}{}
	}

	return set
}

// ServiceOf returns the service portion of a full gRPC method name
// ("/pkg.Service/Method"), or "" when the name is malformed.
func ServiceOf(fullMethod string) string {
	trimmed := strings.TrimPrefix(fullMethod, "/")
	slash := strings.LastIndex(trimmed, "/")
	if slash < 0 {
		return ""
	}

	return trimmed[:slash]
}

// Allows reports whether the set contains service.
func (s serviceSet) Allows(service string) bool {
	_, ok := s[service]
	return ok
}
