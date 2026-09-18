package codecserver_test

import (
	"slices"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/stretchr/testify/require"

	"github.com/temporalio/temporal-proxy/internal/codecserver"
	"github.com/temporalio/temporal-proxy/internal/metrics"
)

// wantSurface is the metric contract the codec server publishes. Operators build
// dashboards on these, so a diff here means a rename that needs a release note,
// not a test to update quietly.
//
// There is deliberately no namespace label, and route is the matched pattern
// rather than the request path: the namespace-prefixed routes put a namespace in
// the path, so labeling by either would be unbounded.
var wantSurface = map[string][]string{
	"tmprl_proxy_codec_server_requests_total":           {"code", "route"},
	"tmprl_proxy_codec_server_request_duration_seconds": {"route"},
}

func TestReporterSurface(t *testing.T) {
	t.Parallel()

	reg := prometheus.NewRegistry()
	r := codecserver.NewReporter(
		metrics.New("tmprl_proxy", promauto.With(reg)).ForSubsystem("codec_server"),
	)

	// A series with no observations is not gathered, so emit one of each first.
	r.Request("/decode", 200, 0.01)

	mfs, err := reg.Gather()
	require.NoError(t, err)

	got := map[string][]string{}
	for _, mf := range mfs {
		names := []string{}
		for _, m := range mf.GetMetric() {
			for _, l := range m.GetLabel() {
				names = append(names, l.GetName())
			}
		}

		slices.Sort(names)
		got[mf.GetName()] = slices.Compact(names)
	}

	require.Equal(t, wantSurface, got)
}
