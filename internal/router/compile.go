package router

import (
	"fmt"
	"strings"

	"github.com/temporalio/temporal-proxy/internal/config"
	"github.com/temporalio/temporal-proxy/pkg/match"
)

// CompileMux compiles routing configuration into a Mux. An empty rule namespace
// matches every namespace, and metadata keys are lowercased to match canonical
// gRPC metadata.
func CompileMux(r config.Routing) (*Mux, error) {
	rules := make([]Rule, 0, len(r.Rules))
	for i, rr := range r.Rules {
		p := rr.Match.Namespace
		if p == "" {
			p = "*"
		}

		ns, err := match.Compile(p)
		if err != nil {
			return nil, fmt.Errorf("routing: rules[%d].match.namespace: %w", i, err)
		}

		meta := make(map[string]match.Matcher, len(rr.Match.Metadata))
		seen := make(map[string]string, len(rr.Match.Metadata))
		for k, v := range rr.Match.Metadata {
			lk := strings.ToLower(k)
			if prev, ok := seen[lk]; ok {
				return nil, fmt.Errorf(
					"routing: rules[%d].match.metadata: keys %q and %q both map to %q when lowercased",
					i, prev, k, lk,
				)
			}

			seen[lk] = k
			m, err := match.Compile(v)
			if err != nil {
				return nil, fmt.Errorf("routing: rules[%d].match.metadata[%q]: %w", i, k, err)
			}

			meta[lk] = m
		}

		rules = append(rules, Rule{
			upstream: rr.Upstream,
			ns:       ns,
			meta:     meta,
		})
	}

	return New(r.DefaultUpstream, r.SystemUpstream, rules...), nil
}
