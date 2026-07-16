package metrics_test

import (
	"strings"
	"testing"

	goprom "github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
	"go.uber.org/fx"

	"github.com/temporalio/temporal-proxy/internal/metrics"
	"github.com/temporalio/temporal-proxy/pkg/logger"
)

func TestFactoryNewCounter(t *testing.T) {
	t.Parallel()

	t.Run("prefixes the bound namespace and registers", func(t *testing.T) {
		t.Parallel()

		reg := goprom.NewRegistry()
		m := metrics.New("myns", promauto.With(reg))

		m.NewCounter(goprom.CounterOpts{
			Subsystem: "sub",
			Name:      "things_total",
			Help:      "Things counted.",
		}, []string{"kind"}).WithLabelValues("a").Inc()

		require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
# HELP myns_sub_things_total Things counted.
# TYPE myns_sub_things_total counter
myns_sub_things_total{kind="a"} 1
`), "myns_sub_things_total"))
	})

	t.Run("overrides a caller-set namespace", func(t *testing.T) {
		t.Parallel()

		reg := goprom.NewRegistry()
		m := metrics.New("bound", promauto.With(reg))

		// The caller's Namespace must be ignored in favor of the bound one, so the
		// series is bound_count_total and never ignored_count_total.
		m.NewCounter(goprom.CounterOpts{
			Namespace: "ignored",
			Name:      "count_total",
			Help:      "Counted.",
		}, nil).WithLabelValues().Inc()

		require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
# HELP bound_count_total Counted.
# TYPE bound_count_total counter
bound_count_total 1
`), "bound_count_total"))

		n, err := testutil.GatherAndCount(reg, "ignored_count_total")
		require.NoError(t, err)
		require.Zero(t, n, "caller-set namespace must not leak into the metric name")
	})

	t.Run("ForSubsystem scopes the subsystem without mutating the parent", func(t *testing.T) {
		t.Parallel()

		reg := goprom.NewRegistry()
		parent := metrics.New("app", promauto.With(reg))
		sub := parent.ForSubsystem("router")

		// The derived factory forces its subsystem, overriding any the caller sets.
		sub.NewCounter(goprom.CounterOpts{
			Subsystem: "ignored",
			Name:      "hits_total",
			Help:      "Hits.",
		}, nil).WithLabelValues().Inc()

		// The parent keeps no subsystem, proving ForSubsystem did not mutate it.
		parent.NewCounter(goprom.CounterOpts{
			Name: "starts_total",
			Help: "Starts.",
		}, nil).WithLabelValues().Inc()

		require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
# HELP app_router_hits_total Hits.
# TYPE app_router_hits_total counter
app_router_hits_total 1
`), "app_router_hits_total"))

		require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
# HELP app_starts_total Starts.
# TYPE app_starts_total counter
app_starts_total 1
`), "app_starts_total"))
	})
}

func TestModuleProvidesNamespacedMetrics(t *testing.T) {
	t.Parallel()

	reg := goprom.NewRegistry()

	var m *metrics.Factory
	app := fx.New(
		fx.Supply(
			fx.Annotate(freeAddr(t), metrics.AddrTag),
			fx.Annotate("wired", metrics.NamespaceTag),
			fx.Annotate(reg, fx.As(new(goprom.Registerer))),
			fx.Annotate(reg, fx.As(new(goprom.Gatherer))),
			fx.Annotate(logger.NewNoopLogger(), fx.As(new(logger.Logger))),
		),
		metrics.Module,
		fx.Populate(&m),
		fx.NopLogger,
	)
	require.NoError(t, app.Err())
	require.NotNil(t, m)

	// The provider must feed the "metricsNamespace" value into New, so a counter
	// built from the injected Metrics carries the "wired" prefix.
	m.NewCounter(goprom.CounterOpts{Name: "wired_total", Help: "Wired."}, nil).WithLabelValues().Inc()

	require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
# HELP wired_wired_total Wired.
# TYPE wired_wired_total counter
wired_wired_total 1
`), "wired_wired_total"))
}
