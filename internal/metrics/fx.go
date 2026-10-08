package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"go.uber.org/fx"

	"github.com/temporalio/temporal-proxy/internal/config"
	"github.com/temporalio/temporal-proxy/pkg/logger"
)

// Module provides a namespaced [Factory] bound to the injected Prometheus
// registry and serves the registry at /metrics on the address the injected
// config names. Any configured fixed labels are stamped onto the Factory's
// registerer rather than onto each collector, so every collector declared
// through it carries them and the runtime's own go_* and process_* series,
// which register directly, do not. Consumers inject the [Factory] to declare
// their collectors, which auto-register under the configured namespace.
//
// The metrics server binds when the app starts, so a taken port fails startup.
// If it stops serving for any reason other than a clean shutdown, the whole app
// is brought down with a non-zero exit code.
var Module = fx.Options(
	fx.Provide(func(p MetricsParams) *Factory {
		return New(
			p.Config.Metrics.Namespace,
			promauto.With(WithFixedLabels(p.Registerer, p.Config.Metrics.Labels.Fixed)),
		)
	}),
	fx.Provide(newServer),
	fx.Invoke(func(*Server) {}),
)

// MetricsParams holds the fx-injected dependencies needed to run the metrics
// HTTP server and build the namespaced [Factory]. Config supplies the listen
// address and the Prometheus prefix through its Metrics block. Registerer is
// where collectors register and Gatherer is what the /metrics handler scrapes;
// supplying both lets callers (and tests) choose between the package-global
// registry and an isolated one.
type MetricsParams struct {
	fx.In
	Lifecycle  fx.Lifecycle
	Shutdowner fx.Shutdowner

	Config *config.Config
	Logger logger.Logger

	Gatherer   prometheus.Gatherer
	Registerer prometheus.Registerer
}
