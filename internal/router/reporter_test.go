package router_test

import (
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"

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

		m.Decision("primary", router.OutcomeMatch)
		m.Decision("primary", router.OutcomeMatch)
		m.Decision("unknown", router.OutcomeUnroutable)
		m.ForwardingError("primary", "no_connection")

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
		m.Decision("secondary", router.OutcomeMatch)

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
