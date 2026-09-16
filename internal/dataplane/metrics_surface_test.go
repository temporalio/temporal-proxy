package dataplane_test

import (
	"slices"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"

	"github.com/temporalio/temporal-proxy/internal/config"
	"github.com/temporalio/temporal-proxy/internal/dataplane"
	"github.com/temporalio/temporal-proxy/internal/metrics"
	"github.com/temporalio/temporal-proxy/internal/router"
)

// wantSurface is the metric contract a wired dataplane publishes: every series
// it emits, with that series' label names. Operators build dashboards on these,
// so a diff here means a rename or a reshape that needs a release note, not a
// test to update quietly.
var wantSurface = map[string][]string{
	"tmprl_proxy_router_decisions_total":                {"outcome", "upstream"},
	"tmprl_proxy_router_forwarding_errors_total":        {"reason", "upstream"},
	"tmprl_proxy_server_requests_total":                 {"code", "method"},
	"tmprl_proxy_server_request_duration_seconds":       {"method"},
	"tmprl_proxy_encryption_vault_ops_total":            {"namespace", "operation", "result"},
	"tmprl_proxy_encryption_vault_ops_duration_seconds": {"namespace", "operation"},
}

// wantMetadataSurface is the same contract for a dataplane running one metadata
// label. It reshapes the published series rather than adding series of its own,
// so the shape an operator with one configured reads off /metrics is a contract
// too. Spelled out rather than derived from wantSurface, so a mistake
// in the derivation cannot hide one in the surface.
// wantFixedSurface is the contract for a dataplane running one fixed label. It
// reshapes every request-scoped series, the same way a metadata label does; that
// it also reshapes the KEK and DEK series, which these reporters do not build,
// is pinned end to end instead. Spelled out rather than derived from wantSurface
// for the same reason as above.
var wantFixedSurface = map[string][]string{
	"tmprl_proxy_router_decisions_total":                {"outcome", "region", "upstream"},
	"tmprl_proxy_router_forwarding_errors_total":        {"reason", "region", "upstream"},
	"tmprl_proxy_server_requests_total":                 {"code", "method", "region"},
	"tmprl_proxy_server_request_duration_seconds":       {"method", "region"},
	"tmprl_proxy_encryption_vault_ops_total":            {"namespace", "operation", "region", "result"},
	"tmprl_proxy_encryption_vault_ops_duration_seconds": {"namespace", "operation", "region"},
}

var wantMetadataSurface = map[string][]string{
	"tmprl_proxy_router_decisions_total":                {"outcome", "tenant", "upstream"},
	"tmprl_proxy_router_forwarding_errors_total":        {"reason", "tenant", "upstream"},
	"tmprl_proxy_server_requests_total":                 {"code", "method", "tenant"},
	"tmprl_proxy_server_request_duration_seconds":       {"method", "tenant"},
	"tmprl_proxy_encryption_vault_ops_total":            {"namespace", "operation", "result", "tenant"},
	"tmprl_proxy_encryption_vault_ops_duration_seconds": {"namespace", "operation", "tenant"},
}

func TestReportersMetricSurface(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		namespaceLabels bool
		wantNamespace   string
	}{
		{name: "namespace labels off", namespaceLabels: false, wantNamespace: ""},
		{name: "namespace labels on", namespaceLabels: true, wantNamespace: "ns1"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			reg := prometheus.NewRegistry()
			driveReporters(t, reg, config.Metrics{
				Labels: config.MetricLabels{Namespace: tt.namespaceLabels},
			}, nil)

			// The label set is declared the same way either way; only the
			// namespace value changes, so dashboards keep the same shape.
			require.Equal(t, wantSurface, surfaceOf(t, reg))
			require.Equal(
				t,
				tt.wantNamespace,
				labelValueOf(t, reg, "tmprl_proxy_encryption_vault_ops_total", "namespace"),
			)
		})
	}
}

func TestReportersMetricSurfaceWithMetadataLabels(t *testing.T) {
	t.Parallel()

	reg := prometheus.NewRegistry()
	driveReporters(t, reg, config.Metrics{
		Labels: config.MetricLabels{
			Namespace: true,
			Metadata:  []config.MetricLabel{{Header: "X-Tenant", Name: "tenant"}},
		},
	}, metadata.Pairs("x-tenant", "acme"))

	require.Equal(t, wantMetadataSurface, surfaceOf(t, reg))

	// Both hops, because they read the value from different places: the gateway
	// reads what it received, and vault_ops reads what the gateway forwarded.
	require.Equal(t, "acme", labelValueOf(t, reg, "tmprl_proxy_server_requests_total", "tenant"))
	require.Equal(t, "acme", labelValueOf(t, reg, "tmprl_proxy_encryption_vault_ops_total", "tenant"))
}

func TestReportersMetricSurfaceWithFixedLabels(t *testing.T) {
	t.Parallel()

	reg := prometheus.NewRegistry()
	driveReporters(t, reg, config.Metrics{
		Labels: config.MetricLabels{Fixed: map[string]string{"region": "us-west-2"}},
	}, nil)

	require.Equal(t, wantFixedSurface, surfaceOf(t, reg))
	require.Equal(
		t,
		"us-west-2",
		labelValueOf(t, reg, "tmprl_proxy_server_requests_total", "region"),
	)
}

// driveReporters wires a full set of reporters for m over reg and records one
// observation on each, since a Vec child only appears once observed. md is the
// request metadata every observation is recorded under, and may be nil: a
// metadata label reads its value from the incoming metadata, so that is the
// only way one reaches a reporter.
func driveReporters(t *testing.T, reg *prometheus.Registry, m config.Metrics, md metadata.MD) {
	t.Helper()

	cfg := &config.Config{Metrics: m, Upstreams: config.UpstreamList{{Name: "cloud"}}}

	f := metrics.New("tmprl_proxy", promauto.With(metrics.WithFixedLabels(reg, m.Labels.Fixed)))
	reps, err := dataplane.NewReporters(f, cfg, true)
	require.NoError(t, err)

	ctx := metadata.NewIncomingContext(t.Context(), md)

	reps.Router.Decision(ctx, "cloud", router.OutcomeMatch)
	reps.Router.ForwardingError(ctx, "cloud", "no_connection")
	reps.Server.Observe(ctx, "/svc/Method", codes.OK, time.Millisecond)
	reps.Encryption.VaultOp(ctx, "encrypt", "success", "ns1", 0.01)
}

// surfaceOf maps every gathered metric family to its sorted label names.
func surfaceOf(t *testing.T, reg *prometheus.Registry) map[string][]string {
	t.Helper()

	mfs, err := reg.Gather()
	require.NoError(t, err)

	out := make(map[string][]string, len(mfs))
	for _, mf := range mfs {
		require.NotEmpty(t, mf.GetMetric(), "no series for %s", mf.GetName())

		names := make([]string, 0, len(mf.GetMetric()[0].GetLabel()))
		for _, l := range mf.GetMetric()[0].GetLabel() {
			names = append(names, l.GetName())
		}

		slices.Sort(names)
		out[mf.GetName()] = names
	}

	return out
}

// labelValueOf returns the value of the named label on the first series of the
// named family, failing the test when either is absent.
func labelValueOf(t *testing.T, reg *prometheus.Registry, name, label string) string {
	t.Helper()

	mfs, err := reg.Gather()
	require.NoError(t, err)

	for _, mf := range mfs {
		if mf.GetName() != name {
			continue
		}

		for _, l := range mf.GetMetric()[0].GetLabel() {
			if l.GetName() == label {
				return l.GetValue()
			}
		}

		t.Fatalf("%s has no %s label", name, label)
	}

	t.Fatalf("no series named %s was gathered", name)

	return ""
}
