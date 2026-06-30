package server

import (
	"context"
	"net"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/encoding"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"

	"github.com/temporalio/temporal-proxy/internal/transport/creds"
	"github.com/temporalio/temporal-proxy/pkg/logger"
	"github.com/temporalio/temporal-proxy/pkg/logger/tag"
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
		logger     logger.Logger
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
		creds              Credentials
		healthCheck        HealthCheck
		logger             logger.Logger
		unaryInterceptors  []grpc.UnaryServerInterceptor
		streamInterceptors []grpc.StreamServerInterceptor
		services           []func(grpc.ServiceRegistrar)
		unknownHandler     grpc.StreamHandler
		serverCodec        encoding.CodecV2
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
		logger:      logger.Default(),
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

	for _, register := range opts.services {
		register(svr)
	}

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

// WithUnaryInterceptor appends unary server interceptors. They are chained in
// the order supplied across all calls and run before the handler.
func WithUnaryInterceptor(in ...grpc.UnaryServerInterceptor) Option {
	return optFunc(func(o *options) { o.unaryInterceptors = append(o.unaryInterceptors, in...) })
}

// WithStreamInterceptor appends stream server interceptors. They are chained in
// the order supplied across all calls and run before the handler.
func WithStreamInterceptor(in ...grpc.StreamServerInterceptor) Option {
	return optFunc(func(o *options) { o.streamInterceptors = append(o.streamInterceptors, in...) })
}

// WithService registers gRPC services on the server. The callback receives the
// underlying server as a grpc.ServiceRegistrar, so callers register via the
// generated pb.RegisterXxxServer(reg, impl) functions.
func WithService(fn func(grpc.ServiceRegistrar)) Option {
	return optFunc(func(o *options) { o.services = append(o.services, fn) })
}

// WithUnknownServiceHandler installs a catch-all handler invoked for any method
// that is not a locally registered service. Used to transparently forward
// unmatched requests.
func WithUnknownServiceHandler(h grpc.StreamHandler) Option {
	return optFunc(func(o *options) { o.unknownHandler = h })
}

// WithServerCodec forces the codec used for all messages on this server. A
// pass-through codec paired with WithUnknownServiceHandler enables transparent
// proxying while locally registered services keep working via codec delegation.
func WithServerCodec(c encoding.CodecV2) Option {
	return optFunc(func(o *options) { o.serverCodec = c })
}

// WithHealthCheck sets the [HealthCheck] used to drive the gRPC health
// service's serving status.
func WithHealthCheck(hc HealthCheck) Option {
	return optFunc(func(o *options) { o.healthCheck = hc })
}

// WithLogger sets the logger used by the server.
func WithLogger(log logger.Logger) Option {
	return optFunc(func(o *options) { o.logger = log })
}

// Start serves on lis and blocks until the server stops. It also kicks off
// the periodic health check, which runs until ctx is cancelled or [Server.Stop]
// is called.
func (s *Server) Start(ctx context.Context, lis net.Listener) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	s.mu.Lock()
	s.logger = s.logger.With(tag.Stringer("addr", lis.Addr()))
	if !s.creds.Encrypted() {
		s.logger.Warn("Running with insecure credentials. Configure TLS for production use.")
	}

	s.cancelFunc = cancel
	log := s.logger
	s.mu.Unlock()

	log.Info("Starting the server")
	go s.runHealthCheck(ctx)

	// Serve returns a non-nil error only when it stops for a reason other than
	// a graceful stop (GracefulStop makes it return nil), so anything here is a
	// genuine failure worth surfacing.
	if err := s.grpcSvr.Serve(lis); err != nil {
		log.Error("Server stopped serving", tag.Error(err))
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

	opts := []grpc.ServerOption{creds}
	if len(o.unaryInterceptors) > 0 {
		opts = append(opts, grpc.ChainUnaryInterceptor(o.unaryInterceptors...))
	}

	if len(o.streamInterceptors) > 0 {
		opts = append(opts, grpc.ChainStreamInterceptor(o.streamInterceptors...))
	}

	if o.unknownHandler != nil {
		opts = append(opts, grpc.UnknownServiceHandler(o.unknownHandler))
	}

	if o.serverCodec != nil {
		opts = append(opts, grpc.ForceServerCodecV2(o.serverCodec))
	}

	return opts, nil
}

func (f optFunc) apply(o *options) {
	f(o)
}
