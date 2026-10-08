package router_test

import (
	"fmt"
	"maps"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/temporalio/temporal-proxy/internal/config"
	"github.com/temporalio/temporal-proxy/internal/router"
)

func TestMuxFor(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		routing config.Routing
		ns      string
		md      map[string][]string
		want    string
		outcome router.Outcome
	}{
		{
			name: "namespace glob matches",
			routing: config.Routing{
				DefaultUpstream: "fallback",
				Rules: []config.RoutingRule{
					{Upstream: "prod", Match: config.RoutingMatch{Namespace: "prod-*"}},
				},
			},
			ns:      "prod-1",
			want:    "prod",
			outcome: router.OutcomeMatch,
		},
		{
			name: "no rule falls through to default",
			routing: config.Routing{
				DefaultUpstream: "fallback",
				Rules: []config.RoutingRule{
					{Upstream: "prod", Match: config.RoutingMatch{Namespace: "prod-*"}},
				},
			},
			ns:      "staging-1",
			want:    "fallback",
			outcome: router.OutcomeDefault,
		},
		{
			name: "empty namespace goes to the system upstream",
			routing: config.Routing{
				DefaultUpstream: "fallback",
				SystemUpstream:  "sys",
			},
			ns:      "",
			want:    "sys",
			outcome: router.OutcomeSystem,
		},
		{
			name: "empty rule namespace matches everything",
			routing: config.Routing{
				Rules: []config.RoutingRule{
					{Upstream: "gold", Match: config.RoutingMatch{Metadata: map[string]string{"x-tier": "gold"}}},
				},
			},
			ns:      "anything",
			md:      map[string][]string{"x-tier": {"gold"}},
			want:    "gold",
			outcome: router.OutcomeMatch,
		},
		{
			name: "metadata key is lowercased to match canonical gRPC metadata",
			routing: config.Routing{
				Rules: []config.RoutingRule{
					{Upstream: "gold", Match: config.RoutingMatch{
						Namespace: "*",
						Metadata:  map[string]string{"X-Tier": "gold"},
					}},
				},
			},
			ns:      "anything",
			md:      map[string][]string{"x-tier": {"gold"}},
			want:    "gold",
			outcome: router.OutcomeMatch,
		},
		{
			name: "no rule and no default is unroutable",
			routing: config.Routing{
				Rules: []config.RoutingRule{
					{Upstream: "prod", Match: config.RoutingMatch{Namespace: "prod-*"}},
				},
			},
			ns:      "staging-1",
			want:    "",
			outcome: router.OutcomeUnroutable,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			mux := router.MuxFor(prepared(t, tt.routing))

			got, outcome := mux.Switch(tt.ns, tt.md)
			require.Equal(t, tt.want, got)
			require.Equal(t, tt.outcome, outcome)
		})
	}
}

// prepared returns r after preparing it inside a minimal config with one
// upstream per name r references.
func prepared(t *testing.T, r config.Routing) config.Routing {
	t.Helper()

	names := map[string]struct{}{"primary": {}}
	for _, n := range []string{r.DefaultUpstream, r.SystemUpstream} {
		if n != "" {
			names[n] = struct{}{}
		}
	}

	for _, rr := range r.Rules {
		names[rr.Upstream] = struct{}{}
	}

	cfg := &config.Config{Listen: config.ListenConfig{HostPort: ":8080"}, Routing: r}
	for i, n := range slices.Sorted(maps.Keys(names)) {
		cfg.Upstreams = append(cfg.Upstreams, config.Upstream{
			Name:   n,
			Listen: config.ListenConfig{HostPort: fmt.Sprintf("127.0.0.1:%d", 7233+i)},
		})
	}

	require.NoError(t, cfg.Prepare())

	return cfg.Routing
}
