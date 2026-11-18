package config_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/api/workflowservice/v1"
	"google.golang.org/grpc"

	"go.temporal.io/proxy/internal/config"
	"go.temporal.io/proxy/internal/dataplane/dataplanetest"
)

// TestNexusRoutabilityMatchesTheDataplane guards the copy of the router's
// logic that config validation uses to check a connector block's namespace:
// for every routing config and namespace, config must name the upstream the
// dataplane's ConnFor actually sends to, and call a namespace unroutable
// exactly when ConnFor refuses it.
func TestNexusRoutabilityMatchesTheDataplane(t *testing.T) {
	t.Parallel()

	namespaces := []string{"orders", "nexus-partners", "payments", "other"}

	tests := []struct {
		name    string
		routing config.Routing
	}{
		{
			name: "namespace rules then a default",
			routing: config.Routing{
				DefaultUpstream: "b",
				Rules:           []config.RoutingRule{{Upstream: "a", Match: config.RoutingMatch{Namespace: "orders"}}},
			},
		},
		{
			name: "a glob rule and no default",
			routing: config.Routing{
				Rules: []config.RoutingRule{{Upstream: "a", Match: config.RoutingMatch{Namespace: "nexus-*"}}},
			},
		},
		{
			name: "a metadata-only rule ahead of a namespace rule",
			routing: config.Routing{
				Rules: []config.RoutingRule{
					{Upstream: "a", Match: config.RoutingMatch{Metadata: map[string]string{"x-tenant": "*"}}},
					{Upstream: "b", Match: config.RoutingMatch{Namespace: "*"}},
				},
			},
		},
		{
			name: "a rule on namespace and metadata ahead of a default",
			routing: config.Routing{
				DefaultUpstream: "b",
				Rules: []config.RoutingRule{{Upstream: "a", Match: config.RoutingMatch{
					Namespace: "*",
					Metadata:  map[string]string{"x-tenant": "*"},
				}}},
			},
		},
		{
			name: "the first matching namespace rule wins",
			routing: config.Routing{
				Rules: []config.RoutingRule{
					{Upstream: "b", Match: config.RoutingMatch{Namespace: "pay*"}},
					{Upstream: "a", Match: config.RoutingMatch{Namespace: "*"}},
				},
			},
		},
		{
			name:    "a default upstream only",
			routing: config.Routing{DefaultUpstream: "a"},
		},
		{
			name: "no route but a system upstream",
			routing: config.Routing{
				SystemUpstream: "a",
				Rules:          []config.RoutingRule{{Upstream: "b", Match: config.RoutingMatch{Namespace: "orders"}}},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			ups := map[string]*dataplanetest.Upstream{
				"a": dataplanetest.NewUpstream(t),
				"b": dataplanetest.NewUpstream(t),
			}

			cfg := &config.Config{
				Routing: tc.routing,
				Upstreams: config.UpstreamList{
					{Name: "a", Listen: ups["a"].Listen()},
					{Name: "b", Listen: ups["b"].Listen()},
				},
			}

			dp := dataplanetest.Start(t, cfg).Dataplane()

			for _, ns := range namespaces {
				want := config.NamespaceUpstream(&cfg.Routing, ns)

				conn, err := dp.ConnFor(ns)
				if want == "" {
					require.Error(t, err, "config calls %q unroutable but ConnFor accepts it", ns)

					continue
				}

				require.NoError(t, err, "config routes %q to %q but ConnFor refuses it", ns, want)

				before := map[string]int{"a": len(ups["a"].Requests()), "b": len(ups["b"].Requests())}

				ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
				_, err = workflowservice.NewWorkflowServiceClient(conn).StartNexusOperationExecution(ctx,
					&workflowservice.StartNexusOperationExecutionRequest{Namespace: ns, OperationId: "op"},
					grpc.WaitForReady(true))
				cancel()
				require.NoError(t, err)

				for name, up := range ups {
					grew := len(up.Requests()) > before[name]
					require.Equal(t, name == want, grew,
						"config routes %q to %q; upstream %q received it: %v", ns, want, name, grew)
				}
			}
		})
	}
}
