package metrics

import (
	"context"
	"errors"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/fx"

	"github.com/temporalio/temporal-proxy/pkg/logger"
	"github.com/temporalio/temporal-proxy/pkg/logger/tag"
)

var (
	// AddrTag annotates the host:port the metrics HTTP server listens on,
	// supplied to fx as the named value "metricsAddr".
	AddrTag = fx.ResultTags(`name:"metricsAddr"`)

	// NamespaceTag annotates the Prometheus namespace prefixed onto every
	// collector, supplied to fx as the named value "metricsNamespace".
	NamespaceTag = fx.ResultTags(`name:"metricsNamespace"`)
)

// Module provides a namespaced [Factory] bound to the injected Prometheus
// registry and serves the registry at /metrics on the address named
// "metricsAddr". Consumers inject the [Factory] to declare their collectors,
// which auto-register under the configured namespace, and should pre-resolve
// labeled handles once at setup rather than per request to keep the emit path
// lock-free and allocation-free.
//
// The HTTP server is bound to the fx lifecycle: it starts in a background
// goroutine on OnStart and shuts down gracefully on OnStop. If the server
// stops for any reason other than a clean shutdown, the whole app is brought
// down with a non-zero exit code.
var Module = fx.Options(
	fx.Provide(func(p MetricsParams) *Factory {
		return New(p.Namespace, promauto.With(p.Registerer))
	}),
	fx.Invoke(func(p MetricsParams) error {
		if p.Addr == "" {
			return errors.New("metrics addr not set")
		}

		mux := http.NewServeMux()
		mux.Handle("/metrics", promhttp.HandlerFor(p.Gatherer, promhttp.HandlerOpts{
			Registry: p.Registerer,
		}))

		svr := &http.Server{
			Addr:              p.Addr,
			Handler:           mux,
			ReadHeaderTimeout: 5 * time.Second,
			ReadTimeout:       10 * time.Second,
		}

		log := p.Logger.With(
			tag.Component("metrics"),
			tag.String("addr", p.Addr),
		)

		p.Lifecycle.Append(fx.Hook{
			OnStart: func(context.Context) error {
				go func() {
					defer func() { _ = svr.Close() }()

					log.Info("Starting metrics server")
					if err := svr.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
						log.Error("Failed to run metrics server", tag.Error(err))
						_ = p.Shutdowner.Shutdown(fx.ExitCode(1))
					}
				}()

				return nil
			},
			OnStop: func(ctx context.Context) error {
				log.Info("Shutting down metrics server")
				if err := svr.Shutdown(ctx); err != nil && !errors.Is(err, http.ErrServerClosed) {
					return err
				}

				return nil
			},
		})

		return nil
	}),
)

// MetricsParams holds the fx-injected dependencies needed to run the metrics
// HTTP server and build the namespaced [Factory]. Addr is the named
// "metricsAddr" listen address and Namespace is the named "metricsNamespace"
// Prometheus prefix. Registerer is where collectors register and Gatherer is
// what the /metrics handler scrapes; supplying both lets callers (and tests)
// choose between the package-global registry and an isolated one.
type MetricsParams struct {
	fx.In
	Lifecycle  fx.Lifecycle
	Shutdowner fx.Shutdowner

	Addr      string `name:"metricsAddr"`
	Namespace string `name:"metricsNamespace"`
	Logger    logger.Logger

	Gatherer   prometheus.Gatherer
	Registerer prometheus.Registerer
}
