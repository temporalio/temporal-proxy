package router_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/temporalio/temporal-proxy/internal/config"
	"github.com/temporalio/temporal-proxy/internal/router"
)

func TestCompileMux(t *testing.T) {
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

			mux, err := router.CompileMux(tt.routing)
			require.NoError(t, err)

			got, outcome := mux.Switch(tt.ns, tt.md)
			require.Equal(t, tt.want, got)
			require.Equal(t, tt.outcome, outcome)
		})
	}
}

func TestCompileMuxErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		routing config.Routing
		wantErr string
	}{
		{
			name: "interior wildcard in a namespace",
			routing: config.Routing{
				Rules: []config.RoutingRule{
					{Upstream: "prod", Match: config.RoutingMatch{Namespace: "a*b"}},
				},
			},
			wantErr: `rules[0].match.namespace`,
		},
		{
			name: "interior wildcard in a metadata value",
			routing: config.Routing{
				Rules: []config.RoutingRule{
					{Upstream: "prod", Match: config.RoutingMatch{
						Namespace: "*",
						Metadata:  map[string]string{"x-tier": "a*b"},
					}},
				},
			},
			wantErr: `rules[0].match.metadata["x-tier"]`,
		},
		{
			name: "metadata keys colliding once lowercased",
			routing: config.Routing{
				Rules: []config.RoutingRule{
					{Upstream: "prod", Match: config.RoutingMatch{
						Namespace: "*",
						Metadata:  map[string]string{"X-Tier": "gold", "x-tier": "silver"},
					}},
				},
			},
			wantErr: `both map to "x-tier" when lowercased`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := router.CompileMux(tt.routing)
			require.ErrorContains(t, err, tt.wantErr)
		})
	}
}
