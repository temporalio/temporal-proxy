package e2e

import (
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
	"go.temporal.io/api/common/v1"
	"go.temporal.io/api/query/v1"
	"go.temporal.io/api/workflowservice/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	"github.com/temporalio/temporal-proxy/internal/config"
	"github.com/temporalio/temporal-proxy/internal/dataplane/dataplanetest"
)

// TestEndToEndMetricTags drives a payload-carrying call through the full stack
// with a metrics tag configured, and proves the caller's metadata reaches the
// labels of every request-scoped collector.
//
// The vault_ops assertion is the one that matters: that collector lives on the
// per-upstream hop, on the far side of a unix socket that context values do not
// cross. It passes only because the value is read from the metadata the gateway
// forwards, and it is what fails if this is ever reworked into a single
// interceptor stashing values in a context.
func TestEndToEndMetricTags(t *testing.T) {
	t.Parallel()

	up := dataplanetest.NewUpstream(t)

	cfg := dataplanetest.Config(up)
	// Mixed case on purpose: gRPC canonicalizes metadata keys, so a config
	// written this way still has to match what arrives on the wire.
	cfg.Metrics.Tags = []config.MetricTag{{Header: "X-Tenant", Label: "tenant"}}
	// Encryption is what makes the per-upstream hop emit vault_ops at all.
	cfg.Encryption = config.Encryption{
		Enabled: true,
		Default: &config.KeyPolicy{URI: testingKeyURI(t), Duration: time.Hour},
	}

	f := dataplanetest.StartApp(t, cfg)

	ctx := metadata.AppendToOutgoingContext(f.Context(), "x-tenant", "acme")
	_, err := f.Client().QueryWorkflow(ctx, queryWithPayload(), grpc.WaitForReady(true))
	require.NoError(t, err)

	// Hop 1: the gateway's own interceptor and the routing decision.
	requireLabel(t, f, "test_server_requests_total", "tenant", "acme")
	requireLabel(t, f, "test_router_decisions_total", "tenant", "acme")

	// Hop 2: across the socket.
	requireLabel(t, f, "test_encryption_vault_ops_total", "tenant", "acme")
}

// TestEndToEndMetricTagsWithoutTheHeader proves a configured label is always
// present, empty when the caller sent nothing, rather than the series being
// reported without it.
func TestEndToEndMetricTagsWithoutTheHeader(t *testing.T) {
	t.Parallel()

	up := dataplanetest.NewUpstream(t)

	cfg := dataplanetest.Config(up)
	cfg.Metrics.Tags = []config.MetricTag{{Header: "x-tenant", Label: "tenant"}}
	cfg.Encryption = config.Encryption{
		Enabled: true,
		Default: &config.KeyPolicy{URI: testingKeyURI(t), Duration: time.Hour},
	}

	f := dataplanetest.StartApp(t, cfg)

	_, err := f.Client().QueryWorkflow(f.Context(), queryWithPayload(), grpc.WaitForReady(true))
	require.NoError(t, err)

	requireLabel(t, f, "test_server_requests_total", "tenant", "")
	requireLabel(t, f, "test_router_decisions_total", "tenant", "")
	requireLabel(t, f, "test_encryption_vault_ops_total", "tenant", "")
}

// queryWithPayload is a QueryWorkflow request carrying one payload, so the
// encryption interceptor on the per-upstream hop has something to seal.
func queryWithPayload() *workflowservice.QueryWorkflowRequest {
	return &workflowservice.QueryWorkflowRequest{
		Namespace: "ns1",
		Execution: &common.WorkflowExecution{WorkflowId: "wf-1"},
		Query: &query.WorkflowQuery{
			QueryType: "state",
			QueryArgs: &common.Payloads{Payloads: []*common.Payload{{
				Metadata: map[string][]byte{wireEncoding: []byte("json/plain")},
				Data:     []byte(`"hi"`),
			}}},
		},
	}
}

// requireLabel asserts that some series in the named metric family carries
// label=want. It compares against the gathered registry rather than a scrape,
// which is equivalent: the metrics module hands that same Gatherer to its
// /metrics handler.
func requireLabel(t *testing.T, f *dataplanetest.Fixture, family, label, want string) {
	t.Helper()

	found, err := testutil.GatherAndLint(f.Gatherer(), family)
	require.NoError(t, err)
	require.Empty(t, found, "collector %s failed prometheus linting", family)

	mfs, err := f.Gatherer().Gather()
	require.NoError(t, err)

	for _, mf := range mfs {
		if mf.GetName() != family {
			continue
		}

		for _, m := range mf.GetMetric() {
			for _, lp := range m.GetLabel() {
				if lp.GetName() == label && lp.GetValue() == want {
					return
				}
			}
		}

		t.Fatalf("metric family %s has no series with %s=%q, got %v", family, label, want, mf.GetMetric())
	}

	t.Fatalf("metric family %s was never registered", family)
}
