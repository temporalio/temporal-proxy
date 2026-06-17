package server

import (
	"context"
	"net"
	"sync"
	"time"

	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"

	"github.com/temporalio/temporal-proxy/internal/transport/creds"
)

type (
	// Server is a gRPC server with a built-in health service and a
	// configurable periodic health check.
	Server struct {
		grpcSvr   *grpc.Server
		healthSvr *health.Server

		creds       Credentials
		healthCheck HealthCheck

		// mu guards logger and cancelFunc, which Start writes from its own
		// goroutine while Stop reads them from the caller's goroutine.
		mu         sync.Mutex
		cancelFunc context.CancelFunc
		logger     log.Logger
	}

	// Credentials produces the [grpc.ServerOption] used to configure
	// transport security for inbound connections and reports whether that
	// transport is encrypted.
	Credentials interface {
		ServerOption() (grpc.ServerOption, error)
		Encrypted() bool
	}

	// Option configures a [Server] at construction time.
	Option interface {
		apply(*options)
	}

	options struct {
		creds       Credentials
		healthCheck HealthCheck
		logger      log.Logger
	}

	optFunc func(*options)
)

// New constructs a [Server]. When no options are supplied, it uses insecure
// credentials, a default health check that always reports SERVING, and a CLI
// logger.
func New(sopts ...Option) (*Server, error) {
	opts := &options{
		creds:       creds.NewInsecure(),
		healthCheck: defaultHealthCheck(),
		logger:      log.NewCLILogger(),
	}
	for _, opt := range sopts {
		opt.apply(opts)
	}

	svrOpts, err := opts.serverOptions()
	if err != nil {
		return nil, err
	}

	svr := grpc.NewServer(svrOpts...)

	// add health check
	hc := health.NewServer()
	grpc_health_v1.RegisterHealthServer(svr, hc)

	return &Server{
		grpcSvr:     svr,
		healthSvr:   hc,
		creds:       opts.creds,
		healthCheck: opts.healthCheck,
		logger:      opts.logger,
	}, nil
}

// WithCredentials sets the transport credentials used for inbound connections.
func WithCredentials(creds Credentials) Option {
	return optFunc(func(o *options) { o.creds = creds })
}

// WithHealthCheck sets the [HealthCheck] used to drive the gRPC health
// service's serving status.
func WithHealthCheck(hc HealthCheck) Option {
	return optFunc(func(o *options) { o.healthCheck = hc })
}

// WithLogger sets the logger used by the server.
func WithLogger(log log.Logger) Option {
	return optFunc(func(o *options) { o.logger = log })
}

// Start serves on lis and blocks until the server stops. It also kicks off
// the periodic health check, which runs until ctx is cancelled or [Server.Stop]
// is called.
func (s *Server) Start(ctx context.Context, lis net.Listener) error {
	ctx, cancel := context.WithCancel(ctx)

	s.mu.Lock()
	s.logger = log.With(s.logger, tag.NewStringerTag("addr", lis.Addr()))
	if !s.creds.Encrypted() {
		s.logger.Warn("Running with insecure credentials. Configure TLS for production use.")
	}

	s.cancelFunc = cancel
	logger := s.logger
	s.mu.Unlock()

	logger.Info("Starting the server")

	go s.runHealthCheck(ctx)

	// Serve returns a non-nil error only when it stops for a reason other than
	// a graceful stop (GracefulStop makes it return nil), so anything here is a
	// genuine failure worth surfacing.
	if err := s.grpcSvr.Serve(lis); err != nil {
		logger.Error("Server stopped serving", tag.Error(err))
		return err
	}

	return nil
}

// Stop gracefully shuts the server down, halting the health check loop and
// waiting for in-flight RPCs to complete.
func (s *Server) Stop(ctx context.Context) error {
	s.mu.Lock()
	logger := s.logger
	cancel := s.cancelFunc
	s.mu.Unlock()

	logger.Info("Shutting down")
	if cancel != nil {
		cancel()
	}

	s.grpcSvr.GracefulStop()
	return nil
}

func (s *Server) runHealthCheck(ctx context.Context) {
	next := grpc_health_v1.HealthCheckResponse_SERVING

	for {
		s.healthSvr.SetServingStatus("", next)

		select {
		case <-ctx.Done():
			s.healthSvr.Shutdown()
			return
		case <-time.After(s.healthCheck.Interval()):
			next = s.healthCheck.Status(ctx)
		}
	}
}

func (o *options) serverOptions() ([]grpc.ServerOption, error) {
	creds, err := o.creds.ServerOption()
	if err != nil {
		return nil, err
	}

	return []grpc.ServerOption{creds}, nil
}

func (f optFunc) apply(o *options) {
	f(o)
}
