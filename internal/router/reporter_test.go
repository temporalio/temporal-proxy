package router_test

import (
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/metadata"

	"github.com/temporalio/temporal-proxy/internal/config"
	"github.com/temporalio/temporal-proxy/internal/metrics"
	"github.com/temporalio/temporal-proxy/internal/router"
)

func TestReporter(t *testing.T) {
	t.Parallel()

	t.Run("pre-resolves meaningful series to zero", func(t *testing.T) {
		t.Parallel()

		_, reg := newTestReporter(t, "primary")

		const wantDecisions = `
# HELP tmprl_proxy_router_decisions_total Total routing decisions, labeled by chosen upstream and outcome.
# TYPE tmprl_proxy_router_decisions_total counter
tmprl_proxy_router_decisions_total{outcome="default",upstream="primary"} 0
tmprl_proxy_router_decisions_total{outcome="match",upstream="primary"} 0
tmprl_proxy_router_decisions_total{outcome="system",upstream="primary"} 0
tmprl_proxy_router_decisions_total{outcome="unroutable",upstream="unknown"} 0
`
		require.NoError(t, testutil.GatherAndCompare(
			reg, strings.NewReader(wantDecisions), "tmprl_proxy_router_decisions_total",
		))

		const wantErrors = `
# HELP tmprl_proxy_router_forwarding_errors_total Total router-originated forwarding failures, labeled by upstream and reason.
# TYPE tmprl_proxy_router_forwarding_errors_total counter
tmprl_proxy_router_forwarding_errors_total{reason="no_connection",upstream="primary"} 0
tmprl_proxy_router_forwarding_errors_total{reason="stream_setup",upstream="primary"} 0
`
		require.NoError(t, testutil.GatherAndCompare(
			reg, strings.NewReader(wantErrors), "tmprl_proxy_router_forwarding_errors_total",
		))
	})

	t.Run("records decisions and forwarding errors", func(t *testing.T) {
		t.Parallel()

		m, reg := newTestReporter(t, "primary")

		m.Decision(t.Context(), "primary", router.OutcomeMatch)
		m.Decision(t.Context(), "primary", router.OutcomeMatch)
		m.Decision(t.Context(), "unknown", router.OutcomeUnroutable)
		m.ForwardingError(t.Context(), "primary", "no_connection")

		const wantDecisions = `
# HELP tmprl_proxy_router_decisions_total Total routing decisions, labeled by chosen upstream and outcome.
# TYPE tmprl_proxy_router_decisions_total counter
tmprl_proxy_router_decisions_total{outcome="default",upstream="primary"} 0
tmprl_proxy_router_decisions_total{outcome="match",upstream="primary"} 2
tmprl_proxy_router_decisions_total{outcome="system",upstream="primary"} 0
tmprl_proxy_router_decisions_total{outcome="unroutable",upstream="unknown"} 1
`
		require.NoError(t, testutil.GatherAndCompare(
			reg, strings.NewReader(wantDecisions), "tmprl_proxy_router_decisions_total",
		))

		const wantErrors = `
# HELP tmprl_proxy_router_forwarding_errors_total Total router-originated forwarding failures, labeled by upstream and reason.
# TYPE tmprl_proxy_router_forwarding_errors_total counter
tmprl_proxy_router_forwarding_errors_total{reason="no_connection",upstream="primary"} 1
tmprl_proxy_router_forwarding_errors_total{reason="stream_setup",upstream="primary"} 0
`
		require.NoError(t, testutil.GatherAndCompare(
			reg, strings.NewReader(wantErrors), "tmprl_proxy_router_forwarding_errors_total",
		))
	})

	t.Run("falls back for an unknown upstream", func(t *testing.T) {
		t.Parallel()

		m, reg := newTestReporter(t, "primary")

		// "secondary" was not pre-resolved; the defensive WithLabelValues path must
		// still create and increment the series.
		m.Decision(t.Context(), "secondary", router.OutcomeMatch)

		const want = `
# HELP tmprl_proxy_router_decisions_total Total routing decisions, labeled by chosen upstream and outcome.
# TYPE tmprl_proxy_router_decisions_total counter
tmprl_proxy_router_decisions_total{outcome="default",upstream="primary"} 0
tmprl_proxy_router_decisions_total{outcome="match",upstream="primary"} 0
tmprl_proxy_router_decisions_total{outcome="match",upstream="secondary"} 1
tmprl_proxy_router_decisions_total{outcome="system",upstream="primary"} 0
tmprl_proxy_router_decisions_total{outcome="unroutable",upstream="unknown"} 0
`
		require.NoError(t, testutil.GatherAndCompare(
			reg, strings.NewReader(want), "tmprl_proxy_router_decisions_total",
		))
	})
}

func TestReporterTags(t *testing.T) {
	t.Parallel()

	tags := metrics.NewTags([]config.MetricTag{{Header: "X-Tenant", Label: "tenant"}})

	t.Run("pre-resolves nothing, since a tag value arrives with a request", func(t *testing.T) {
		t.Parallel()

		_, reg := newTaggedTestReporter(t, tags, "primary")

		// The counterpart to "pre-resolves meaningful series to zero": a handle
		// would have to fix a tag value, so none is taken and no series exists
		// until something is actually recorded.
		count, err := testutil.GatherAndCount(reg, "tmprl_proxy_router_decisions_total")
		require.NoError(t, err)
		require.Zero(t, count)
	})

	t.Run("records the tag on both collectors", func(t *testing.T) {
		t.Parallel()

		m, reg := newTaggedTestReporter(t, tags, "primary")

		ctx := metadata.NewIncomingContext(t.Context(), metadata.Pairs("x-tenant", "acme"))
		m.Decision(ctx, "primary", router.OutcomeMatch)
		m.ForwardingError(ctx, "primary", "no_connection")

		// A request without the header still reports the label, empty.
		m.Decision(t.Context(), "primary", router.OutcomeMatch)

		const wantDecisions = `
# HELP tmprl_proxy_router_decisions_total Total routing decisions, labeled by chosen upstream and outcome.
# TYPE tmprl_proxy_router_decisions_total counter
tmprl_proxy_router_decisions_total{outcome="match",tenant="",upstream="primary"} 1
tmprl_proxy_router_decisions_total{outcome="match",tenant="acme",upstream="primary"} 1
`
		require.NoError(t, testutil.GatherAndCompare(
			reg, strings.NewReader(wantDecisions), "tmprl_proxy_router_decisions_total",
		))

		const wantErrors = `
# HELP tmprl_proxy_router_forwarding_errors_total Total router-originated forwarding failures, labeled by upstream and reason.
# TYPE tmprl_proxy_router_forwarding_errors_total counter
tmprl_proxy_router_forwarding_errors_total{reason="no_connection",tenant="acme",upstream="primary"} 1
`
		require.NoError(t, testutil.GatherAndCompare(
			reg, strings.NewReader(wantErrors), "tmprl_proxy_router_forwarding_errors_total",
		))
	})
}
