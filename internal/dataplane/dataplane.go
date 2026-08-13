package dataplane

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/reflect/protoregistry"

	"github.com/temporalio/temporal-proxy/internal/auth"
	"github.com/temporalio/temporal-proxy/internal/auth/outbound"
	"github.com/temporalio/temporal-proxy/internal/config"
	"github.com/temporalio/temporal-proxy/internal/metrics"
	"github.com/temporalio/temporal-proxy/internal/protoutil"
	"github.com/temporalio/temporal-proxy/internal/proxy"
	"github.com/temporalio/temporal-proxy/internal/router"
	"github.com/temporalio/temporal-proxy/internal/server"
	"github.com/temporalio/temporal-proxy/internal/services"
	"github.com/temporalio/temporal-proxy/internal/transport/connect"
	"github.com/temporalio/temporal-proxy/internal/transport/socket"
	"github.com/temporalio/temporal-proxy/pkg/crypto"
	"github.com/temporalio/temporal-proxy/pkg/logger"
)

type (
	// Dataplane is the assembled request path. It is single-use: not restartable
	// after Stop.
	Dataplane struct {
		ctx       context.Context
		gateway   *server.Server
		hostPort  string
		upstreams []*upstreamTier
		ready     []*connect.Conn
		abort     func(error)
		abortOnce sync.Once
		logger    logger.Logger

		mu        sync.Mutex
		addr      net.Addr
		listeners []net.Listener
		stopping  bool
	}

	// Option configures a [Dataplane] via [New].
	Option func(*options)

	options struct {
		extractor  *protoutil.Extractor
		translator *protoutil.Translator
		types      protoutil.Types
		pool       *connect.Pool
		metrics    *metrics.Factory
		allowlist  services.Allowlist
		auth       auth.Authenticator
		vault      *crypto.Vault
		logger     logger.Logger
		abort      func(error)
	}

	// upstreamTier is one upstream's proxy and the socket it binds.
	upstreamTier struct {
		name string
		path string
		svr  *proxy.Server
	}
)

// New validates cfg in full, compiles the routing table, derives each upstream's
// socket path once, and builds both tiers. ctx is long lived and drives each
// tier's health check; the context passed to Start bounds startup only. Neither
// stops serving, which only Stop does. New binds nothing and dials nothing. Every
// Prometheus collector is registered here, so New must be called once per
// registry.
func New(ctx context.Context, cfg *config.Config, opts ...Option) (*Dataplane, error) {
	o := &options{logger: logger.Default(), types: protoregistry.GlobalTypes}
	for _, opt := range opts {
		opt(o)
	}

	if err := validate(ctx, cfg, o); err != nil {
		return nil, err
	}

	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("invalid configuration: %w", err)
	}

	// Encryption is a security control: if it is enabled, or keys are configured
	// even while disabled for new traffic, but no vault reached the dataplane,
	// fail fast. Keys without a vault means payloads sealed earlier cannot be
	// opened, which is as dangerous as forwarding cleartext outbound. The check
	// is on the concrete pointer, before any conversion to an interface, since a
	// nil pointer in a non-nil interface would slip past it.
	if (cfg.Encryption.Enabled || cfg.Encryption.Default != nil) && o.vault == nil {
		return nil, errors.New("encryption is enabled or keys are configured, but no vault was provided")
	}

	reps, err := newReporters(o.metrics, cfg, o.vault != nil)
	if err != nil {
		return nil, err
	}

	mux, err := router.CompileMux(cfg.Routing)
	if err != nil {
		return nil, err
	}

	dp := &Dataplane{
		ctx:      ctx,
		hostPort: cfg.Listen.HostPort,
		abort:    o.abort,
		logger:   o.logger,
	}

	conns := make(map[string]grpc.ClientConnInterface, len(cfg.Upstreams))
	for i := range cfg.Upstreams {
		up := &cfg.Upstreams[i]

		path, err := socket.UnixPath(up.Listen.HostPort)
		if err != nil {
			return nil, fmt.Errorf("failed to resolve proxy socket path[%q]: %w", up.Name, err)
		}

		tier, ready, err := newUpstreamTier(cfg, o, up, path, reps)
		if err != nil {
			return nil, err
		}

		dp.upstreams = append(dp.upstreams, tier)
		if ready != nil {
			dp.ready = append(dp.ready, ready)
		}

		// The gateway dials this socket. Creating the connection does not open
		// one, so nothing connects until the proxy has bound it.
		sock := "unix://" + path
		conn, err := o.pool.ConnOrCreate(sock, sock, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return nil, fmt.Errorf("failed to create upstream client[%q]: %w", up.Name, err)
		}

		conns[up.Name] = conn
	}

	handler := router.Handler(
		router.NewDirector(mux, conns, reps.router, o.logger),
		o.extractor,
		o.allowlist,
		reps.router,
	)

	gateway, err := server.New(
		server.WithCredentials(cfg.Listen.TLS.Listener()),
		server.WithServerCodec(router.Codec()),
		// Health entries come from the allowlist, so what the gateway reports a
		// status for is exactly what it will forward.
		server.WithHealthServices(o.allowlist.ServiceNames()...),
		server.WithStreamInterceptor(reps.server.StreamInterceptor()),
		server.WithStreamInterceptor(auth.StreamServerInterceptor(o.auth, o.logger)),
		server.WithUnknownServiceHandler(handler),
		server.WithLogger(o.logger),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create server: %w", err)
	}

	dp.gateway = gateway

	return dp, nil
}

// WithExtractor sets the extractor the gateway reads routing fields with.
// Required.
func WithExtractor(e *protoutil.Extractor) Option {
	return Option(func(o *options) { o.extractor = e })
}

// WithTranslator sets the translator each upstream rewrites namespaces with.
// Required.
func WithTranslator(t *protoutil.Translator) Option {
	return Option(func(o *options) { o.translator = t })
}

// WithProtoTypes sets the type registry messages are resolved against,
// defaulting to [protoregistry.GlobalTypes]. A nil registry keeps the default,
// so an absent optional dependency can be passed straight through.
func WithProtoTypes(t protoutil.Types) Option {
	return Option(func(o *options) {
		if t != nil {
			o.types = t
		}
	})
}

// WithPool sets the connection pool the dataplane creates upstream connections
// from. The pool's lifecycle stays with the caller. Required.
func WithPool(p *connect.Pool) Option {
	return Option(func(o *options) { o.pool = p })
}

// WithMetrics sets the factory every collector is registered with. Required.
func WithMetrics(f *metrics.Factory) Option {
	return Option(func(o *options) { o.metrics = f })
}

// WithAllowlist sets the allowlist that decides which services may be
// forwarded. Required.
func WithAllowlist(a services.Allowlist) Option {
	return Option(func(o *options) { o.allowlist = a })
}

// WithAuth sets the authenticator applied to inbound gateway requests.
// Required.
func WithAuth(a auth.Authenticator) Option {
	return Option(func(o *options) { o.auth = a })
}

// WithVault sets the vault used to seal and open payloads. Omit it when no
// encryption keys are configured; New rejects enabled encryption without one.
func WithVault(v *crypto.Vault) Option {
	return Option(func(o *options) { o.vault = v })
}

// WithLogger sets the logger used by the dataplane and both tiers, defaulting to
// [logger.Default]. A nil logger keeps the default, so an absent optional
// dependency can be passed straight through.
func WithLogger(log logger.Logger) Option {
	return Option(func(o *options) {
		if log != nil {
			o.logger = log
		}
	})
}

// WithAbort sets a function called at most once, from the goroutine that was
// serving, when a tier stops for a reason other than Stop. It must not block and
// must not call back into the Dataplane.
func WithAbort(fn func(error)) Option {
	return Option(func(o *options) { o.abort = fn })
}

// Addr is the address the gateway is accepting on, nil before Start.
func (d *Dataplane) Addr() net.Addr {
	d.mu.Lock()
	defer d.mu.Unlock()

	return d.addr
}

// SocketPath is the unix path the named upstream's proxy binds and the gateway
// dials. It is the single derivation of that path.
func (d *Dataplane) SocketPath(upstream string) (string, error) {
	for _, up := range d.upstreams {
		if up.name == upstream {
			return up.path, nil
		}
	}

	return "", fmt.Errorf("dataplane: no upstream named %q", upstream)
}

// validate reports the first required dependency that is missing, by field
// name, so a wiring mistake fails at construction rather than as a nil
// dereference on the first request.
func validate(ctx context.Context, cfg *config.Config, o *options) error {
	switch {
	case ctx == nil:
		return errors.New("dataplane: ctx is required")
	case cfg == nil:
		return errors.New("dataplane: cfg is required")
	case o.extractor == nil:
		return errors.New("dataplane: WithExtractor is required")
	case o.translator == nil:
		return errors.New("dataplane: WithTranslator is required")
	case o.pool == nil:
		return errors.New("dataplane: WithPool is required")
	case o.metrics == nil:
		return errors.New("dataplane: WithMetrics is required")
	case o.allowlist == nil:
		return errors.New("dataplane: WithAllowlist is required")
	case o.auth == nil:
		return errors.New("dataplane: WithAuth is required")
	}

	return nil
}

// newUpstreamTier builds one upstream's proxy. It returns the tier, the
// connection to open eagerly at start (nil for a templated upstream, which
// resolves per request and has nothing to open yet), and any error.
func newUpstreamTier(
	cfg *config.Config,
	o *options,
	up *config.Upstream,
	path string,
	reps *reporters,
) (*upstreamTier, *connect.Conn, error) {
	// Request-independent dial options: namespace translation and outbound
	// credentials. Per-request credentials are added by the resolver.
	var dialOpts []grpc.DialOption
	rules := &up.Namespaces.Rules
	if rules.Configured() {
		dialOpts = append(dialOpts, proxy.TranslationDialOptions(o.translator, rules.Remote, rules.Local)...)
	}

	cp, err := outbound.CredentialProviderFor(up.Credentials)
	if err != nil {
		return nil, nil, fmt.Errorf("invalid credentials for upstream %q: %w", up.Name, err)
	}
	if cp != nil {
		dialOpts = append(dialOpts, outbound.DialOptions(cp)...)
	}

	// A vault is present whenever encryption keys are configured, which may be
	// true even when encryption is disabled; install the interceptor whenever one
	// is present and pass Enabled so sealing is gated while inbound decryption
	// always runs. That keeps payloads sealed earlier openable after encryption
	// is turned off for new traffic. Added after translation so it is the
	// innermost unary interceptor, sealing outbound payloads last and opening
	// inbound payloads first.
	if o.vault != nil {
		enc, err := proxy.EncryptionInterceptor(cfg.Encryption.Enabled, o.vault, reps.encryption)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to build encryption interceptor for upstream %q: %w", up.Name, err)
		}

		dialOpts = append(dialOpts, grpc.WithChainUnaryInterceptor(enc))
	}

	res, err := proxy.ResolverFor(up, dialOpts, o.logger)
	if err != nil {
		return nil, nil, err
	}

	conn, err := connect.NewConn(o.pool.ConnOrCreate, res)
	if err != nil {
		return nil, nil, err
	}

	fw, err := proxy.NewForwarder(conn, o.allowlist, proxy.WithProtoTypes(o.types))
	if err != nil {
		return nil, nil, err
	}

	svr, err := proxy.New(
		up.Listen.HostPort,
		fw,
		proxy.WithLogger(o.logger),
		proxy.WithSocketPath(path),
	)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create proxy for upstream %q: %w", up.Name, err)
	}

	// Only a static upstream holds a connection worth opening before serving; a
	// templated one resolves its target per request.
	var ready *connect.Conn
	if res.IsStatic() {
		ready = conn
	}

	return &upstreamTier{name: up.Name, path: path, svr: svr}, ready, nil
}
