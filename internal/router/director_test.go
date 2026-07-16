package router

import (
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/temporalio/temporal-proxy/internal/metrics"
)

func TestDirectorResolve(t *testing.T) {
	t.Parallel()

	t.Run("default decision with a connection", func(t *testing.T) {
		t.Parallel()

		reg := prometheus.NewRegistry()
		d := &director{
			conns:    map[string]*grpc.ClientConn{"primary": {}},
			mux:      New("primary", "", Rule{upstream: "primary", ns: matchPrefix("prod-")}),
			reporter: testReporter(reg),
		}

		target, err := d.Resolve(t.Context(), "/svc/M", "other", nil)
		require.NoError(t, err)
		require.Equal(t, "primary", target.Upstream)
		require.NotNil(t, target.Conn)

		requireDecisions(t, reg, `
# HELP tmprl_proxy_router_decisions_total Total routing decisions, labeled by chosen upstream and outcome.
# TYPE tmprl_proxy_router_decisions_total counter
tmprl_proxy_router_decisions_total{outcome="default",upstream="primary"} 1
tmprl_proxy_router_decisions_total{outcome="match",upstream="primary"} 0
tmprl_proxy_router_decisions_total{outcome="system",upstream="primary"} 0
tmprl_proxy_router_decisions_total{outcome="unroutable",upstream="unknown"} 0
`)
	})

	t.Run("no connection for chosen upstream", func(t *testing.T) {
		t.Parallel()

		reg := prometheus.NewRegistry()
		d := &director{
			conns:    map[string]*grpc.ClientConn{},
			mux:      New("primary", ""),
			reporter: testReporter(reg),
		}

		_, err := d.Resolve(t.Context(), "/svc/M", "other", nil)
		require.Equal(t, codes.Unavailable, status.Code(err))

		requireDecisions(t, reg, `
# HELP tmprl_proxy_router_decisions_total Total routing decisions, labeled by chosen upstream and outcome.
# TYPE tmprl_proxy_router_decisions_total counter
tmprl_proxy_router_decisions_total{outcome="default",upstream="primary"} 1
tmprl_proxy_router_decisions_total{outcome="match",upstream="primary"} 0
tmprl_proxy_router_decisions_total{outcome="system",upstream="primary"} 0
tmprl_proxy_router_decisions_total{outcome="unroutable",upstream="unknown"} 0
`)
		requireErrors(t, reg, `
# HELP tmprl_proxy_router_forwarding_errors_total Total router-originated forwarding failures, labeled by upstream and reason.
# TYPE tmprl_proxy_router_forwarding_errors_total counter
tmprl_proxy_router_forwarding_errors_total{reason="no_connection",upstream="primary"} 1
tmprl_proxy_router_forwarding_errors_total{reason="stream_setup",upstream="primary"} 0
`)
	})

	t.Run("unroutable request", func(t *testing.T) {
		t.Parallel()

		reg := prometheus.NewRegistry()
		d := &director{
			conns:    map[string]*grpc.ClientConn{},
			mux:      New("", ""),
			reporter: testReporter(reg),
		}

		_, err := d.Resolve(t.Context(), "/svc/M", "other", nil)
		require.Equal(t, codes.FailedPrecondition, status.Code(err))

		requireDecisions(t, reg, `
# HELP tmprl_proxy_router_decisions_total Total routing decisions, labeled by chosen upstream and outcome.
# TYPE tmprl_proxy_router_decisions_total counter
tmprl_proxy_router_decisions_total{outcome="default",upstream="primary"} 0
tmprl_proxy_router_decisions_total{outcome="match",upstream="primary"} 0
tmprl_proxy_router_decisions_total{outcome="system",upstream="primary"} 0
tmprl_proxy_router_decisions_total{outcome="unroutable",upstream="unknown"} 1
`)
	})
}

// testReporter builds a Reporter over reg with the production namespace and
// subsystem, so emitted series match the tmprl_proxy_router_* names asserted
// below.
func testReporter(reg *prometheus.Registry) *Reporter {
	return NewReporter(metrics.New("tmprl_proxy", promauto.With(reg)).ForSubsystem("router"), []string{"primary"})
}

func requireDecisions(t *testing.T, g prometheus.Gatherer, want string) {
	t.Helper()
	require.NoError(t, testutil.GatherAndCompare(g, strings.NewReader(want), "tmprl_proxy_router_decisions_total"))
}

func requireErrors(t *testing.T, g prometheus.Gatherer, want string) {
	t.Helper()
	require.NoError(t, testutil.GatherAndCompare(g, strings.NewReader(want), "tmprl_proxy_router_forwarding_errors_total"))
}
