package metrics

import (
	"errors"
	"net/http"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/fx"

	"github.com/temporalio/temporal-proxy/internal/httpserver"
	"github.com/temporalio/temporal-proxy/pkg/logger/tag"
)

// Server serves the Prometheus registry at /metrics on its own port, plaintext
// and unauthenticated. It wraps [httpserver.Server] so it binds when the app
// starts and so the fx graph can tell it apart from the route-group server.
type Server struct {
	*httpserver.Server
}

// newServer builds the metrics server and binds it to the fx lifecycle. A
// serving failure after start shuts the app down with exit code 1. Returns an
// error when no address is configured, since an empty one would bind a random
// port.
func newServer(p MetricsParams) (*Server, error) {
	if p.Config.Metrics.HostPort == "" {
		return nil, errors.New("metrics addr not set")
	}

	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(p.Gatherer, promhttp.HandlerOpts{
		Registry: p.Registerer,
	}))

	svr := httpserver.New(p.Config.Metrics.HostPort, mux, nil, p.Logger.With(tag.Component("metrics")),
		func(error) { _ = p.Shutdowner.Shutdown(fx.ExitCode(1)) })

	p.Lifecycle.Append(fx.Hook{OnStart: svr.Start, OnStop: svr.Stop})

	return &Server{Server: svr}, nil
}
