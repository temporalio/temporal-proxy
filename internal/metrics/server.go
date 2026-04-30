package metrics

import (
	"context"
	"errors"
	"net"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
)

type (
	// Server serves the Prometheus metrics registry over HTTP.
	Server struct {
		http   *http.Server
		logger log.Logger
	}

	// ServerOption configures a Server.
	ServerOption func(*serverOpts)

	serverOpts struct {
		gatherer prometheus.Gatherer
		logger   log.Logger
		path     string
	}
)

// NewServer returns a Server that exposes the given gatherer at path over HTTP.
func NewServer(opts ...ServerOption) *Server {
	sopts := &serverOpts{
		gatherer: prometheus.DefaultGatherer,
		logger:   log.NewNoopLogger(),
		path:     "/metrics",
	}

	for _, opt := range opts {
		opt(sopts)
	}

	mux := http.NewServeMux()
	mux.Handle(sopts.path, promhttp.HandlerFor(sopts.gatherer, promhttp.HandlerOpts{}))

	return &Server{
		http: &http.Server{
			Handler: mux,
			// Timeouts to avoid slowloris et al.
			ReadHeaderTimeout: 5 * time.Second,
			ReadTimeout:       10 * time.Second,
			WriteTimeout:      10 * time.Second,
			IdleTimeout:       30 * time.Second,
		},
		logger: sopts.logger,
	}
}

// Gatherer sets the Prometheus gatherer exposed by the server. Defaults to prometheus.DefaultGatherer.
func Gatherer(g prometheus.Gatherer) ServerOption {
	return func(so *serverOpts) { so.gatherer = g }
}

// Path sets the HTTP path at which metrics are served. Defaults to "/metrics".
func Path(p string) ServerOption {
	return func(so *serverOpts) { so.path = p }
}

// ServerLogger sets the logger used by the server. Defaults to a no-op logger.
func ServerLogger(l log.Logger) ServerOption {
	return func(so *serverOpts) { so.logger = l }
}

// Start begins serving metrics on the provided listener in a background goroutine.
func (s *Server) Start(lis net.Listener) {
	s.logger.Info("Starting metrics server")

	go func() {
		if err := s.http.Serve(lis); err != nil && !errors.Is(err, http.ErrServerClosed) {
			s.logger.Error("Metrics server stopped unexpectedly", tag.Error(err))
		}
	}()
}

// Stop gracefully shuts down the HTTP server, waiting until ctx is cancelled or all connections are closed.
func (s *Server) Stop(ctx context.Context) error {
	s.logger.Info("Stopping metrics server")
	return s.http.Shutdown(ctx)
}
