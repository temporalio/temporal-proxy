package codecserver

import (
	"fmt"
	"maps"
	"slices"

	"github.com/temporalio/temporal-proxy/internal/config"
)

// OverrideMap maps a remote namespace name to the local name a per-namespace
// codec policy is keyed by. It satisfies [Namespaces].
//
// It holds only the namespaces where the answer can differ, which is those
// carrying a per-namespace policy under an upstream that translates names.
// Every other name is left alone, so the map is small and usually empty.
//
// A nil OverrideMap is usable and translates nothing. An OverrideMap is
// read-only after construction and safe for concurrent use.
type OverrideMap map[string]string

// NewOverrideMap builds the remote-to-local namespace mapping from
// configuration, once at startup. Call it before serving; it reads cfg and
// keeps no reference to it.
//
// For each upstream that translates namespaces, it walks the namespaces
// carrying a per-namespace codec policy and records the remote name that
// upstream would have produced. Deriving the mapping forwards, through the
// upstream's own rules, is what makes it exact: it honours an explicit
// local-to-remote override, where inverting a translation would be a lossy
// suffix trim. Identity entries are skipped, so an upstream whose rules do not
// change a name contributes nothing.
//
// Returns a mapping that may be nil when no namespace carries a policy, which
// callers may use directly.
// Returns an error when a remote name is also a policy key, or when two local
// namespaces produce the same remote name. Both are ambiguous, and an
// ambiguous mapping would seal one namespace's payloads under another's key
// policy, so it fails at startup rather than resolving arbitrarily per request.
func NewOverrideMap(cfg *config.Config) (OverrideMap, error) {
	locals := policyNamespaces(cfg)
	if len(locals) == 0 {
		return nil, nil
	}

	keyed := make(map[string]struct{}, len(locals))
	for _, local := range locals {
		keyed[local] = struct{}{}
	}

	out := OverrideMap{}
	for i := range cfg.Upstreams {
		rules := &cfg.Upstreams[i].Namespaces.Rules
		if !rules.Configured() {
			continue
		}

		for _, local := range locals {
			remote := rules.Remote(local)
			if remote == local {
				continue
			}

			if _, ok := keyed[remote]; ok {
				return nil, fmt.Errorf(
					"codecserver: %q is both an encryption override and the remote name of %q, "+
						"so a caller naming it cannot be resolved to one policy",
					remote, local,
				)
			}

			if prev, ok := out[remote]; ok && prev != local {
				return nil, fmt.Errorf(
					"codecserver: remote namespace %q maps to both %q and %q, "+
						"so a caller naming it cannot be resolved to one policy",
					remote, prev, local,
				)
			}

			out[remote] = local
		}
	}

	return out, nil
}

// Local returns the local namespace name for remote, or remote unchanged when
// no override matches. It implements [Namespaces] and has no error path.
//
// Passing an unknown name through is the correct answer rather than a
// fallback. The vault resolves a namespace it holds no key for to the default
// key policy, which is what a namespace with no override should get, and it
// also means a caller that already speaks local names reaches its own override
// directly without the mapping having to recognise it.
//
// Safe for concurrent use. Safe on a nil receiver.
func (m OverrideMap) Local(remote string) string {
	if local, ok := m[remote]; ok {
		return local
	}

	return remote
}

// policyNamespaces returns every namespace carrying a per-namespace codec
// policy, sorted so an error names the same pair on every run. Encryption is the
// only such policy today; a second one is added here rather than at the call
// site.
func policyNamespaces(cfg *config.Config) []string {
	return slices.Sorted(maps.Keys(cfg.Encryption.Overrides))
}
