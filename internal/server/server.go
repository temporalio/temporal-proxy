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

const (
	// defaultShutdownTimeout bounds the drain in [Server.Stop]. The gateway
	// proxies long-poll methods that block 60s+, so a budget that waits them out
	// would stall every restart; dropping them instead is safe because callers
	// re-poll.
	defaultShutdownTimeout = 5 * time.Second

	// minShutdownTimeout keeps a zero or negative [WithShutdownTimeout] from
	// turning Stop into an immediate kill: an already-answered call still flushes.
	minShutdownTimeout = 50 * time.Millisecond
)

type (
	// Server is a gRPC server with a built-in health service and a
	// configurable periodic health check.
	Server struct {
		grpcSvr   *grpc.Server
		healthSvr *health.Server

		creds           Credentials
		healthCheck     HealthCheck
		healthServices  []string
		shutdownTimeout time.Duration

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
		healthServices     []string
		logger             logger.Logger
		shutdownTimeout    time.Duration
		unaryInterceptors  []grpc.UnaryServerInterceptor
		streamInterceptors []grpc.StreamServerInterceptor
		services           []func(grpc.ServiceRegistrar)
		unknownHandler     grpc.StreamHandler
		serverCodec        encoding.CodecV2
	}

	optFunc func(*options)
)

// New constructs a [Server]. When no options are supplied, it uses insecure
// credentials, a default health check that always reports SERVING, a CLI
// logger, and a five second drain budget.
func New(sopts ...Option) (*Server, error) {
	opts := &options{
		creds:           creds.NewListener(creds.Insecure()),
		healthCheck:     defaultHealthCheck(),
		logger:          logger.Default(),
		shutdownTimeout: defaultShutdownTimeout,
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

	// Seeded here rather than in the health loop so every entry exists before the
	// first connection is accepted.
	for _, name := range opts.healthServices {
		hc.SetServingStatus(name, grpc_health_v1.HealthCheckResponse_SERVING)
	}

	for _, register := range opts.services {
		register(svr)
	}

	return &Server{
		grpcSvr:         svr,
		healthSvr:       hc,
		creds:           opts.creds,
		healthCheck:     opts.healthCheck,
		healthServices:  opts.healthServices,
		logger:          opts.logger,
		shutdownTimeout: opts.shutdownTimeout,
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

// WithHealthServices names the services the health service answers for, by proto
// full name. Each gets an entry reporting the same status as the unnamed one.
// Names accumulate across calls.
func WithHealthServices(names ...string) Option {
	return optFunc(func(o *options) { o.healthServices = append(o.healthServices, names...) })
}

// WithLogger sets the logger used by the server.
func WithLogger(log logger.Logger) Option {
	return optFunc(func(o *options) { o.logger = log })
}

// WithShutdownTimeout bounds how long [Server.Stop] waits for in-flight calls
// before dropping them. It defaults to five seconds and is clamped to a 50ms
// floor. Stop also honours its Context, so the effective budget is whichever
// expires first; a Context already cancelled forces immediately.
func WithShutdownTimeout(d time.Duration) Option {
	return optFunc(func(o *options) { o.shutdownTimeout = max(minShutdownTimeout, d) })
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

// Stop shuts the server down, halting the health check loop and draining
// in-flight RPCs. The drain is bounded by whichever expires first: the
// [WithShutdownTimeout] budget or ctx. Past that, remaining calls are dropped.
// A forced shutdown is still a shutdown, so it is reported through a warning
// rather than an error; the only errors here would be a caller's to handle, and
// there are none.
func (s *Server) Stop(ctx context.Context) error {
	s.mu.Lock()
	log := s.logger
	cancel := s.cancelFunc
	s.mu.Unlock()

	log.Info("Shutting down")

	// Flipped here rather than left to the health loop so a probe sees NOT_SERVING
	// before the drain closes the listener, and so a status refresh already in
	// flight cannot push SERVING back out behind it: SetServingStatus is ignored
	// once the health server is shut down.
	s.healthSvr.Shutdown()
	if cancel != nil {
		cancel()
	}

	done := make(chan struct{})
	go func() {
		s.grpcSvr.GracefulStop()
		close(done)
	}()

	// fx runs the remaining OnStop hooks off the same Context, so overrunning it
	// strands them.
	stopCtx, cancelStop := context.WithTimeout(ctx, s.shutdownTimeout)
	defer cancelStop()

	select {
	case <-done:
		log.Info("Server stopped cleanly")
	case <-stopCtx.Done():
		// stopCtx ends on whichever came first, our budget or ctx, so name which.
		// An operator who raises WithShutdownTimeout past the caller's budget
		// otherwise reads a number that never elapsed and tunes the knob that is
		// not binding.
		cause := "shutdown timeout"
		if err := ctx.Err(); err != nil {
			cause = "stop context: " + err.Error()
		}

		log.Warn(
			"Drain ended with calls in flight. Dropping them",
			tag.String("cause", cause),
			tag.String("timeout", s.shutdownTimeout.String()),
		)

		// Closes the transports, so a caller waiting on a call this drain gave up on
		// learns now rather than at its own timeout.
		//
		// Backgrounded because it cannot be relied on to return: GracefulStop waits
		// for handlers while holding the server's lock, which Stop needs, so a
		// handler that never returns wedges both, and Serve with them. That is why
		// Serve is not waited on here and why the deadline is enforced by this
		// select rather than by Stop. The goroutines end with the process, which by
		// this point is what we are.
		go s.grpcSvr.Stop()
	}

	return nil
}

func (s *Server) runHealthCheck(ctx context.Context) {
	next := grpc_health_v1.HealthCheckResponse_SERVING

	for {
		// Every entry reports the same status: the server's health is process-wide.
		s.healthSvr.SetServingStatus("", next)
		for _, name := range s.healthServices {
			s.healthSvr.SetServingStatus(name, next)
		}

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
