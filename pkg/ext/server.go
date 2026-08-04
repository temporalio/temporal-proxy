package ext

import (
	"context"
	"errors"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"

	"github.com/temporalio/temporal-proxy/pkg/api/auth/v1"
	"github.com/temporalio/temporal-proxy/pkg/api/kms/v1"
	"github.com/temporalio/temporal-proxy/pkg/logger"
	"github.com/temporalio/temporal-proxy/pkg/logger/tag"
)

// signals are the signals [Serve] shuts down on. SIGINT arrives as
// [os.Interrupt], the same value, so listing both would register it twice.
var signals = []os.Signal{
	os.Interrupt,
	syscall.SIGTERM,
}

type (
	// Option configures the server built by [Serve]. The set is closed: options is
	// unexported, so a caller cannot name it.
	Option func(*options)

	options struct {
		authHeader      string
		authCheck       CredentialCheck
		hostPort        string
		listener        net.Listener
		auth            Auth
		kms             KMS
		logger          logger.Logger
		shutdownTimeout time.Duration
		serverOptions   []grpc.ServerOption
	}
)

// Serve runs an extension server until ctx is cancelled or the process is
// signalled, then shuts down and returns nil. A non-nil return means the server
// never started, never that a caller was turned away.
//
// Both generated services are registered whether or not [WithAuth] and [WithKMS]
// were given, and one left unset answers Unimplemented. Defaults are :8900 on
// every interface, a five second shutdown grace period, and the transport and
// limits [WithServerOption] describes. Serve blocks and installs its own handler
// for [signals], so a caller with no lifecycle of its own can pass
// [context.Background].
func Serve(ctx context.Context, opts ...Option) error {
	sOpts := &options{
		hostPort:        ":8900",
		logger:          logger.Default(),
		shutdownTimeout: 5 * time.Second,
		serverOptions: []grpc.ServerOption{
			grpc.Creds(insecure.NewCredentials()),
			grpc.MaxConcurrentStreams(128),   // Avoid resource exhaustion
			grpc.MaxRecvMsgSize(1024 * 1024), // Max 1MB messages
			grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
				MinTime:             5 * time.Second, // Don't let clients ping faster than this
				PermitWithoutStream: true,            // Allow pings without active RPCs
			}),
			grpc.KeepaliveParams(keepalive.ServerParameters{
				MaxConnectionIdle:     5 * time.Minute,  // Close idle clients
				MaxConnectionAge:      30 * time.Minute, // Force reconnect once in a while
				MaxConnectionAgeGrace: 10 * time.Second, // Time to finish in-flight RPCs
				Time:                  1 * time.Minute,  // Ping client, if idle >= 1m
				Timeout:               10 * time.Second, // Client must respond within 10s
			}),
		},
	}

	for _, opt := range opts {
		opt(sOpts)
	}

	// Prepended so the guard is outermost and a caller's own interceptors only see
	// admitted calls.
	if sOpts.authCheck != nil {
		// A guard on an unnamed header rejects every call as missing credentials,
		// which is safe but reads as a broken proxy rather than a misconfigured
		// server. Refusing to start says which it is.
		if sOpts.authHeader == "" {
			return errors.New("a header name is required to guard this server")
		}

		sOpts.serverOptions = append([]grpc.ServerOption{
			grpc.ChainUnaryInterceptor(unaryGuard(sOpts.authHeader, sOpts.authCheck)),
		}, sOpts.serverOptions...)
	}

	svr := grpc.NewServer(sOpts.serverOptions...)
	auth.RegisterAuthServiceServer(svr, &authService{auth: sOpts.auth})
	kms.RegisterEncryptionServiceServer(svr, &kmsService{kms: sOpts.kms, log: sOpts.logger})

	return runServer(ctx, svr, sOpts)
}

// WithAddr sets the address to listen on, defaulting to :8900 on every
// interface. Narrow it to loopback where the proxy reaches this server over one:
// an extension server can admit callers and unwrap key material, so it should not
// be published needlessly. Ignored when [WithListener] supplies a listener.
func WithAddr(hostPort string) Option {
	return func(o *options) { o.hostPort = hostPort }
}

// WithAuth registers an implementation of api.auth.v1.AuthService. Without it the
// service answers Unimplemented, and since the proxy fails closed, that denies
// every caller rather than admitting them.
func WithAuth(a Auth) Option {
	return func(o *options) { o.auth = a }
}

// WithListener serves on lis instead of a listener opened from [WithAddr]. Use it
// when something else owns the socket: a test that would rather not bind a port,
// or a process handed one by its supervisor. The server closes lis during
// shutdown, so a caller must not.
func WithListener(lis net.Listener) Option {
	return func(o *options) { o.listener = lis }
}

// WithKMS registers an implementation of api.kms.v1.EncryptionService. Without it
// the service answers Unimplemented, failing every Seal and, more
// consequentially, every Open of a payload already sealed under it.
func WithKMS(kms KMS) Option {
	return func(o *options) { o.kms = kms }
}

// WithLogger sets the logger for the server's lifecycle, defaulting to
// [logger.Default]. Handlers are not given it. A nil logger is ignored rather
// than installed, matching [logger.SetDefault]; the alternative is a panic on the
// first line the server logs.
func WithLogger(l logger.Logger) Option {
	return func(o *options) {
		if l != nil {
			o.logger = l
		}
	}
}

// WithServerAuth guards this server's unary methods with check, which receives
// the single value of the header metadata. It authenticates the proxy to this
// server, unlike the [Auth] service, which is how the proxy asks about somebody
// else. Unary covers the whole surface today, since both generated services are
// unary; adding a streaming method to either proto means pairing this with a
// [grpc.StreamServerInterceptor].
//
// A call is rejected with Unauthenticated unless the header is present exactly
// once and check accepts its value. Repeats are refused rather than searched, so
// a caller cannot spray guesses in one call. Compare in constant time
// ([crypto/subtle.ConstantTimeCompare]) for a shared secret. A nil check installs
// nothing and leaves the server open, while a non-nil one with an empty header
// would reject everything, so [Serve] treats it as a configuration error and
// declines to start.
func WithServerAuth(header string, check CredentialCheck) Option {
	return func(o *options) { o.authHeader, o.authCheck = header, check }
}

// WithServerOption adds gRPC server options, and is the escape hatch for anything
// this package does not name: TLS via [grpc.Creds], a stats handler, a raised
// limit.
//
// Options accumulate, and [Serve] starts with insecure credentials, 128 concurrent
// streams, a 1MiB receive limit sized for key material rather than payloads, and
// keepalive settings that floor client ping intervals. An option given here is
// applied after those, so it wins for any setting gRPC resolves to one value:
// serving TLS is grpc.Creds(credentials.NewTLS(...)), with nothing to clear first.
//
// Interceptor order is visible. The guard [WithServerAuth] installs is chained
// ahead of anything added here, so a [grpc.ChainUnaryInterceptor] sees only
// admitted calls. [grpc.UnaryInterceptor] is gRPC's own exception: prepended ahead
// of the whole chain, it observes calls about to be rejected.
func WithServerOption(opts ...grpc.ServerOption) Option {
	return func(o *options) { o.serverOptions = append(o.serverOptions, opts...) }
}

// WithShutdownTimeout bounds how long shutdown waits for in-flight calls before
// dropping connections. It defaults to five seconds and is clamped to a 50ms
// floor, so a zero or negative value still lets an already-answered call flush.
// Set it below the grace period of whatever supervises the process; overrunning
// that trades the graceful shutdown for a SIGKILL.
func WithShutdownTimeout(t time.Duration) Option {
	return func(o *options) { o.shutdownTimeout = max(50*time.Millisecond, t) }
}

// runServer serves and shuts down on the first of ctx being cancelled or a signal
// arriving. The listener is obtained before the signal handler is installed, so a
// bind failure comes back as Serve's error rather than as a shutdown.
func runServer(ctx context.Context, svr *grpc.Server, opts *options) error {
	lis := opts.listener
	if lis == nil {
		var err error
		if lis, err = (&net.ListenConfig{}).Listen(ctx, "tcp", opts.hostPort); err != nil {
			return err
		}
	}

	ctx, stop := signal.NotifyContext(ctx, signals...)
	defer stop()

	errs := make(chan error, 1)

	// The listener's address rather than the configured one, so a port of 0 and a
	// listener from WithListener both report where the server ended up.
	log := opts.logger.With(tag.String("addr", lis.Addr().String()))
	go func() {
		log.Info("Starting extension server")
		if err := svr.Serve(lis); err != nil {
			errs <- err
		}
	}()

	select {
	case err := <-errs:
		log.Error("Shutting down due to startup error", tag.Error(err))
		return err
	case <-ctx.Done():
		log.Info("Shutdown signal received")

		// GracefulStop has no bound of its own, so it runs where it can be abandoned.
		// A handler blocked on a backend that never answers would otherwise hold the
		// process open until something less patient killed it.
		done := make(chan struct{})
		go func() {
			svr.GracefulStop()
			close(done)
		}()

		select {
		case <-done:
			log.Info("Server stopped cleanly")
		case <-time.After(opts.shutdownTimeout):
			log.Warn("GracefulStop timed out. Forcing shutdown")

			// Backgrounded because GracefulStop holds the server's lock while waiting
			// on the handler that wedged it, so a blocking Stop deadlocks against it
			// and holds the process open past the timeout meant to bound exactly this.
			go svr.Stop()
		}
	}

	return nil
}
