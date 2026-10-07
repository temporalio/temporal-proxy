package codecserver

import (
	"fmt"
	"maps"
	"slices"

	"github.com/temporalio/temporal-proxy/internal/config"
)

// OverrideMap maps a remote namespace name to the local name a per-namespace
// codec policy is keyed by, holding only namespaces with a policy under an
// upstream that translates names. It satisfies [Namespaces].
//
// A nil OverrideMap is usable and translates nothing. An OverrideMap is
// read-only after construction and safe for concurrent use.
type OverrideMap map[string]string

// NewOverrideMap builds the remote-to-local namespace mapping from cfg,
// recording the remote name each translating upstream produces for every
// namespace carrying a per-namespace codec policy. Identity entries are
// skipped, so an upstream whose rules do not change a name contributes nothing.
// It keeps no reference to cfg.
//
// Returns a nil mapping, which is usable, when no namespace carries a policy.
// Returns an error when a remote name is also a policy key, or when two local
// namespaces produce the same remote name, since either mapping is ambiguous.
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
// no override matches. It implements [Namespaces]. Safe for concurrent use and
// on a nil receiver.
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
