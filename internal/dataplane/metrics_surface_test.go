package dataplane_test

import (
	"slices"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"

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
			driveReporters(t, reg, tt.namespaceLabels)

			// The label set is declared the same way either way; only the
			// namespace value changes, so dashboards keep the same shape.
			require.Equal(t, wantSurface, surfaceOf(t, reg))
			require.Equal(t, tt.wantNamespace, namespaceValueOf(t, reg, "tmprl_proxy_encryption_vault_ops_total"))
		})
	}
}

// driveReporters wires a full set of reporters over reg and records one
// observation on each, since a Vec child only appears once observed.
func driveReporters(t *testing.T, reg *prometheus.Registry, namespaceLabels bool) {
	t.Helper()

	cfg := &config.Config{
		Metrics:   config.Metrics{NamespaceLabels: namespaceLabels},
		Upstreams: config.UpstreamList{{Name: "cloud"}},
	}

	reps, err := dataplane.NewReporters(metrics.New("tmprl_proxy", promauto.With(reg)), cfg, true)
	require.NoError(t, err)

	reps.Router.Decision("cloud", router.OutcomeMatch)
	reps.Router.ForwardingError("cloud", "no_connection")
	reps.Server.Observe("/svc/Method", codes.OK, time.Millisecond)
	reps.Encryption.VaultOp("encrypt", "success", "ns1", 0.01)
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

// namespaceValueOf returns the namespace label value on the first series of
// the named family.
func namespaceValueOf(t *testing.T, reg *prometheus.Registry, name string) string {
	t.Helper()

	mfs, err := reg.Gather()
	require.NoError(t, err)

	for _, mf := range mfs {
		if mf.GetName() != name {
			continue
		}

		for _, l := range mf.GetMetric()[0].GetLabel() {
			if l.GetName() == "namespace" {
				return l.GetValue()
			}
		}

		t.Fatalf("%s has no namespace label", name)
	}

	t.Fatalf("no series named %s was gathered", name)

	return ""
}
