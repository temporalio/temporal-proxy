package router_test

import (
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/temporalio/temporal-proxy/internal/config"
	"github.com/temporalio/temporal-proxy/internal/router"
	"github.com/temporalio/temporal-proxy/pkg/logger"
	"github.com/temporalio/temporal-proxy/pkg/logger/tag"
)

type fakeConn struct{ grpc.ClientConnInterface }

func TestDirectorResolve(t *testing.T) {
	t.Parallel()

	conn := &fakeConn{}
	mux, err := router.CompileMux(config.Routing{
		DefaultUpstream: "primary",
		Rules: []config.RoutingRule{
			{Upstream: "prod", Match: config.RoutingMatch{Namespace: "prod-*"}},
		},
	})
	require.NoError(t, err)

	rep, reg := newTestReporter(t, "primary", "prod")
	d := router.NewDirector(
		mux,
		map[string]grpc.ClientConnInterface{"primary": conn, "prod": conn},
		rep,
		nil,
	)

	target, err := d.Resolve(t.Context(), "/svc/Method", "prod-1", nil)
	require.NoError(t, err)
	require.Equal(t, "prod", target.Upstream)
	require.Same(t, conn, target.Conn)

	requireDecisions(t, reg, `
tmprl_proxy_router_decisions_total{outcome="default",upstream="primary"} 0
tmprl_proxy_router_decisions_total{outcome="default",upstream="prod"} 0
tmprl_proxy_router_decisions_total{outcome="match",upstream="primary"} 0
tmprl_proxy_router_decisions_total{outcome="match",upstream="prod"} 1
tmprl_proxy_router_decisions_total{outcome="system",upstream="primary"} 0
tmprl_proxy_router_decisions_total{outcome="system",upstream="prod"} 0
tmprl_proxy_router_decisions_total{outcome="unroutable",upstream="unknown"} 0
`)
}

func TestDirectorResolveUnroutable(t *testing.T) {
	t.Parallel()

	mux, err := router.CompileMux(config.Routing{})
	require.NoError(t, err)

	rep, reg := newTestReporter(t)
	d := router.NewDirector(mux, nil, rep, nil)

	_, err = d.Resolve(t.Context(), "/svc/Method", "anything", nil)
	require.Equal(t, codes.FailedPrecondition, status.Code(err))

	// An unroutable request is attributed to the "unknown" upstream, since no
	// upstream was chosen.
	requireDecisions(t, reg, `
tmprl_proxy_router_decisions_total{outcome="unroutable",upstream="unknown"} 1
`)
}

func TestDirectorResolveNoConnection(t *testing.T) {
	t.Parallel()

	mux, err := router.CompileMux(config.Routing{DefaultUpstream: "primary"})
	require.NoError(t, err)

	rep, reg := newTestReporter(t, "primary")
	d := router.NewDirector(mux, nil, rep, nil)

	_, err = d.Resolve(t.Context(), "/svc/Method", "anything", nil)
	require.Equal(t, codes.Unavailable, status.Code(err))
	require.ErrorContains(t, err, `no connection for upstream "primary"`)

	// The decision is recorded before the connection lookup, so a missing
	// connection counts as both a decision and a forwarding error.
	requireDecisions(t, reg, `
tmprl_proxy_router_decisions_total{outcome="default",upstream="primary"} 1
tmprl_proxy_router_decisions_total{outcome="match",upstream="primary"} 0
tmprl_proxy_router_decisions_total{outcome="system",upstream="primary"} 0
tmprl_proxy_router_decisions_total{outcome="unroutable",upstream="unknown"} 0
`)
	requireForwardingErrors(t, reg, `
tmprl_proxy_router_forwarding_errors_total{reason="no_connection",upstream="primary"} 1
tmprl_proxy_router_forwarding_errors_total{reason="stream_setup",upstream="primary"} 0
`)
}

func TestDirectorLogsRoutingDecision(t *testing.T) {
	t.Parallel()

	mux, err := router.CompileMux(config.Routing{DefaultUpstream: "primary"})
	require.NoError(t, err)

	rep, _ := newTestReporter(t, "primary")
	log := logger.NewTestLogger()
	d := router.NewDirector(
		mux,
		map[string]grpc.ClientConnInterface{"primary": &fakeConn{}},
		rep,
		log,
	)

	_, err = d.Resolve(t.Context(), "/svc/Method", "orders", nil)
	require.NoError(t, err)

	// The constructor scopes the logger to the router component, so that tag
	// leads every entry.
	require.True(t, log.ContainsEntry(
		logger.LevelDebug,
		"routing request",
		tag.Component("router"),
		tag.String("method", "/svc/Method"),
		tag.String("namespace", "orders"),
		tag.String("upstream", "primary"),
		tag.Stringer("outcome", router.OutcomeDefault),
	))
}

// requireDecisions asserts the full set of decision series, so a stray
// increment on an unexpected label fails too. want omits the HELP and TYPE
// lines, which are prepended here.
func requireDecisions(t *testing.T, g prometheus.Gatherer, want string) {
	t.Helper()

	const header = `
# HELP tmprl_proxy_router_decisions_total Total routing decisions, labeled by chosen upstream and outcome.
# TYPE tmprl_proxy_router_decisions_total counter`

	require.NoError(t, testutil.GatherAndCompare(
		g, strings.NewReader(header+want), "tmprl_proxy_router_decisions_total",
	))
}

// requireForwardingErrors asserts the full set of forwarding-error series.
func requireForwardingErrors(t *testing.T, g prometheus.Gatherer, want string) {
	t.Helper()

	const header = `
# HELP tmprl_proxy_router_forwarding_errors_total Total router-originated forwarding failures, labeled by upstream and reason.
# TYPE tmprl_proxy_router_forwarding_errors_total counter`

	require.NoError(t, testutil.GatherAndCompare(
		g, strings.NewReader(header+want), "tmprl_proxy_router_forwarding_errors_total",
	))
}
