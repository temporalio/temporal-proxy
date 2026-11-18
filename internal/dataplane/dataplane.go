package dataplane

import (
	"context"
	"errors"
	"fmt"
	"net"
	"slices"
	"strings"
	"sync"

	"google.golang.org/grpc"

	"go.temporal.io/proxy/internal/auth"
	"go.temporal.io/proxy/internal/auth/outbound"
	"go.temporal.io/proxy/internal/cloud/translation"
	"go.temporal.io/proxy/internal/config"
	"go.temporal.io/proxy/internal/proxy"
	"go.temporal.io/proxy/internal/router"
	"go.temporal.io/proxy/internal/server"
	"go.temporal.io/proxy/internal/transport/connect"
	"go.temporal.io/proxy/internal/transport/creds"
	"go.temporal.io/proxy/internal/version"
	"go.temporal.io/proxy/pkg/logger"
	"go.temporal.io/proxy/pkg/logger/tag"
)

type (
	// Dataplane is the assembled request path. It is single-use: not restartable
	// after Stop.
	Dataplane struct {
		ctx       context.Context
		gateway   *server.Server
		hostPort  string
		ready     []*connect.Conn
		codecs    *proxy.Codecs
		abort     Abort
		abortOnce sync.Once
		logger    logger.Logger

		mu       sync.Mutex
		addr     net.Addr
		lis      net.Listener
		stopping bool
	}

	// Abort is called at most once, from the goroutine that was serving, when the
	// gateway stops for a reason other than Stop. It must not block and must not
	// call back into the Dataplane. [Module] provides one that shuts the
	// application down; replace it with fx.Decorate to observe the stop instead.
	// A nil Abort drops the notification, leaving the log entry as the only record.
	Abort func(error)
)

// newDataplane builds the routing mux from the matchers config preparation
// compiled, and builds the gateway and a per-upstream proxy for every upstream.
// p.Context is long lived and drives the gateway's health check; the context
// passed to Start bounds startup only. Neither stops serving, which only Stop
// does. It binds nothing and dials nothing. Every Prometheus collector is
// registered here, so it must run once per registry.
func newDataplane(p Params) (*Dataplane, error) {
	cfg := p.Config

	// Encryption is a security control: if it is enabled, or keys are configured
	// even while disabled for new traffic, but no vault reached the dataplane,
	// fail fast. Keys without a vault means payloads sealed earlier cannot be
	// opened, which is as dangerous as forwarding cleartext outbound. The check
	// is on the concrete pointer, before any conversion to an interface, since a
	// nil pointer in a non-nil interface would slip past it.
	if (cfg.Encryption.Enabled || cfg.Encryption.Default != nil) && p.Vault == nil {
		return nil, errors.New("encryption is enabled or keys are configured, but no vault was provided")
	}

	reps, err := newReporters(p.Metrics, cfg, p.Vault != nil)
	if err != nil {
		return nil, err
	}

	// One pool of decoded bytes for every response, whichever upstream or the
	// codec server it came through, so the bound holds for the proxy as a whole.
	limiter := proxy.NewDecodeLimiter(int64(cfg.Compression.InFlightLimit()))

	// Every upstream applies the same chain: the vault and the encryption switch
	// are global, so nothing here varies per upstream. Building it once is also
	// what lets the codec server apply the identical chain. Its response limit
	// is the largest any upstream accepts, since the codec server serves them
	// all; each forwarder bounds its own responses by its upstream's.
	codecOpts := proxy.CodecOptions{
		Encrypt:             cfg.Encryption.Enabled,
		EncodeFailures:      cfg.Encryption.Failures,
		SkipEncodings:       cfg.Encryption.SkipEncodings,
		Compression:         &cfg.Compression,
		CompressionReporter: reps.compression,
		DecodeLimiter:       limiter,
		ResponseLimit:       int64(cfg.Upstreams.MaxResponseLimit()),
	}

	// Only assign the vault once it is known to be there. p.Vault is a concrete
	// pointer and the field is an interface, so assigning unconditionally would
	// hand over a non-nil interface holding a nil pointer, which reads as a vault
	// being present and panics the first time a payload is opened.
	if p.Vault != nil {
		codecOpts.Vault = p.Vault
		codecOpts.Reporter = reps.encryption
	}

	codecs, err := proxy.NewCodecs(codecOpts)
	if err != nil {
		return nil, fmt.Errorf("failed to build payload codecs: %w", err)
	}

	mux := router.MuxFor(cfg.Routing)

	// A translation block nothing will consult changes nothing and would leave an
	// operator waiting for behaviour that cannot arrive, so say so rather than
	// ignoring it silently.
	if !cfg.APITranslations.CloudAPI.IsZero() && !translates(cfg) {
		p.Logger.Warn(
			"apiTranslations is configured but no upstream is Temporal Cloud, so no method will be translated",
		)
	}

	// With no keys there is no encryption codec, so the list has nothing to skip.
	if len(cfg.Encryption.SkipEncodings) > 0 && p.Vault == nil {
		p.Logger.Warn("encryption.skipEncodings is set but no encryption keys are configured, so it has no effect")
	}

	dp := &Dataplane{
		ctx:      p.Context,
		hostPort: cfg.Listen.HostPort,
		codecs:   codecs,
		abort:    p.Abort,
		logger:   p.Logger,
	}

	handlers := make(map[string]grpc.StreamHandler, len(cfg.Upstreams))
	for i := range cfg.Upstreams {
		up := &cfg.Upstreams[i]

		fw, ready, err := newUpstreamForwarder(&p, up, codecs, limiter)
		if err != nil {
			return nil, err
		}

		if ready != nil {
			dp.ready = append(dp.ready, ready)
		}

		// The gateway calls this upstream's forwarder directly with the stream it
		// accepted.
		handlers[up.Name] = fw.Handle
	}

	handler := router.Handler(
		router.NewDirector(mux, handlers, reps.router, p.Logger),
		p.Allowlist,
	)

	gateway, err := server.New(
		server.WithCredentials(cfg.Listen.Listener(creds.WithReloader(p.Reloader, "gateway"))),
		server.WithServerCodec(router.Codec()),
		// Health entries come from the allowlist, so what the gateway reports a
		// status for is exactly what it will forward.
		server.WithHealthServices(p.Allowlist.ServiceNames()...),
		server.WithLoopbackHealthCheck(cfg.Health.CheckInterval(), cfg.Health.CheckTimeout()),
		server.WithStreamInterceptor(reps.server.StreamInterceptor()),
		// Straight after the reporter, so a panic in any interceptor after it or in
		// the handler is recovered and still recorded as an Internal request.
		server.WithRecoveryHandler(p.Logger, reps.server.Panic),
		// Ahead of authentication: it resolves what the request is addressing, and
		// an authenticator decides on that as well as on the caller's credentials.
		server.WithStreamInterceptor(router.PeekInterceptor(p.Extractor, p.Allowlist)),
		server.WithStreamInterceptor(auth.StreamServerInterceptor(p.Auth, p.Logger)),
		server.WithUnknownServiceHandler(handler),
		server.WithLogger(p.Logger),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create server: %w", err)
	}

	dp.gateway = gateway

	return dp, nil
}

// Addr is the address the gateway is accepting on, nil before Start.
func (d *Dataplane) Addr() net.Addr {
	d.mu.Lock()
	defer d.mu.Unlock()

	return d.addr
}

// Codecs is the payload codec chain every upstream applies, so a caller outside
// the request path transforms payloads exactly as a proxied request would.
func (d *Dataplane) Codecs() *proxy.Codecs {
	return d.codecs
}

// newUpstreamForwarder builds one upstream's forwarder. It returns the
// forwarder, the connection to open eagerly at start (nil for a templated
// upstream, which resolves per request and has nothing to open yet), and any
// error.
func newUpstreamForwarder(
	p *Params,
	up *config.Upstream,
	codecs *proxy.Codecs,
	limiter *proxy.DecodeLimiter,
) (*proxy.Forwarder, *connect.Conn, error) {
	// Request-independent dial options: namespace translation and outbound
	// credentials. Per-request credentials are added by the resolver.
	var dialOpts []grpc.DialOption
	rules := &up.Namespaces.Rules
	if rules.Configured() {
		dialOpts = append(dialOpts, proxy.TranslationDialOptions(p.Translator, rules.Remote, rules.Local)...)
	}

	// Cloud derives its endpoint and authorizes requests by the translated
	// namespace, so report one that cannot work there. Remote is identity when no
	// rules are configured, which still catches a client sending a short name to
	// an upstream that expects fully-qualified ones.
	//
	// Cloud is also told which proxy build the request came from, so a report of
	// something the proxy does wrong can be tied to a release. No other upstream
	// is sent it.
	if up.IsCloud() {
		dialOpts = append(dialOpts, proxy.CloudNamespaceDialOptions(
			rules.Remote,
			p.Logger.With(tag.String("upstream", up.Name)),
		)...)
		dialOpts = append(dialOpts, proxy.VersionDialOptions(version.Version)...)
	}

	cp, err := outbound.CredentialProviderFor(up.Credentials)
	if err != nil {
		return nil, nil, fmt.Errorf("invalid credentials for upstream %q: %w", up.Name, err)
	}
	if cp != nil {
		dialOpts = append(dialOpts, outbound.DialOptions(cp)...)
	}

	// One interceptor applies every payload codec, so anything added to the chain
	// travels this path without another interceptor here. It decides for itself
	// which codecs a direction needs and skips a direction with none, so it is
	// installed unconditionally rather than gated on any one codec's config.
	// Added after translation so it is the innermost unary interceptor, encoding
	// outbound payloads last and decoding inbound payloads first.
	// Payloads are bound to the namespace the upstream knows, so a local rename
	// leaves them readable and two proxies naming one namespace differently agree.
	// An upstream with bindingLabelsFrom takes the label from the named upstream's
	// rules instead, while its requests are still translated with its own.
	var codecOpts []proxy.InterceptorOption
	if labels := p.Config.Upstreams.BindingRules(up); labels.Configured() {
		codecOpts = append(codecOpts, proxy.WithRemoteNamespace(labels.Remote))
	}

	cdc, err := codecs.Interceptor(codecOpts...)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to build codec interceptor for upstream %q: %w", up.Name, err)
	}

	dialOpts = append(dialOpts, grpc.WithChainUnaryInterceptor(cdc))

	// Cloud does not serve every method on a frontend, so the ones it answers
	// elsewhere are translated and sent to its control plane instead, and the ones
	// it answers nowhere are replied to by the proxy itself. Nothing configures
	// that: every Cloud upstream gets the whole registry, since a namespaced method
	// lands on whichever Cloud upstream its namespace routes to, and an entry that
	// never lands on a given upstream simply never fires there. A Temporal Service
	// gets none, since it serves those methods itself.
	//
	// It goes on last so it is the innermost interceptor: the namespace translator
	// and the payload codec above it then see the method and message types the
	// caller asked for, and only the hop onto the wire carries the substitute.
	if up.IsCloud() {
		reg, conn, err := cloudAPIConn(p, up)
		if err != nil {
			return nil, nil, err
		}

		dialOpts = append(dialOpts, translation.DialOptions(reg, translation.Via(conn))...)
	}

	res, err := proxy.ResolverFor(up, dialOpts, p.Logger, creds.WithReloader(p.Reloader, "upstream/"+up.Name))
	if err != nil {
		return nil, nil, err
	}

	conn, err := connect.NewConn(p.Pool.Scoped("upstream/"+up.Name), res, connect.WithConnections(up.Connection.PoolSize(up.IsTemplated())))
	if err != nil {
		return nil, nil, err
	}

	// The budget is held until the response is sent, not just while the codec
	// interceptor decodes it, since the caller is still reading it until then.
	fw, err := proxy.NewForwarder(
		conn,
		proxy.WithProtoTypes(p.Types),
		proxy.WithDecodeBudget(limiter, int64(up.Connection.ResponseLimit())),
	)
	if err != nil {
		return nil, nil, err
	}

	// Only a static upstream holds a connection worth opening before serving; a
	// templated one resolves its target per request.
	var ready *connect.Conn
	if res.IsStatic() {
		ready = conn
	}

	return fw, ready, nil
}

// translates reports whether any upstream will have method translation
// installed, which is any upstream being Temporal Cloud. It is the same question
// newUpstreamForwarder asks of one upstream, so a configuration this answers
// false for installs nothing anywhere.
func translates(cfg *config.Config) bool {
	return slices.ContainsFunc(cfg.Upstreams, func(up config.Upstream) bool {
		return up.IsCloud()
	})
}

// cloudAPIConn builds the connection translated methods for up are answered
// over, along with the translations that use it. The connection is dialled by
// the same resolver and credential machinery as an upstream but is not
// registered with the router; only a translation reaches it. It is not opened
// eagerly, so an unreachable control plane does not stop the proxy serving
// everything else.
func cloudAPIConn(p *Params, up *config.Upstream) (*translation.Registry, *connect.Conn, error) {
	reg, err := translation.Default()
	if err != nil {
		return nil, nil, fmt.Errorf("failed to build method translations: %w", err)
	}

	api, err := p.Config.APITranslations.CloudAPI.Upstream(up)
	if err != nil {
		return nil, nil, err
	}

	// This connection terminates at Cloud too, and carries the proxy's version for
	// the same reason the frontend upstream does. Nothing installs it here: the
	// translation that is the only way to reach this connection is the innermost
	// interceptor on the upstream chain above, so a call arrives having already
	// passed the version interceptor there and travels on with the metadata it
	// stamped. The end-to-end test holds that ordering in place.
	var dialOpts []grpc.DialOption
	cp, err := outbound.CredentialProviderFor(api.Credentials)
	if err != nil {
		return nil, nil, fmt.Errorf("invalid credentials for the Cloud API of upstream %q: %w", up.Name, err)
	}
	if cp != nil {
		dialOpts = append(dialOpts, outbound.DialOptions(cp)...)
	}

	// Every Cloud upstream renders the same Cloud API block, so they share one
	// file set, and the reloader reads it once under this one id.
	res, err := proxy.ResolverFor(api, dialOpts, p.Logger, creds.WithReloader(p.Reloader, "cloud_api"))
	if err != nil {
		return nil, nil, fmt.Errorf("failed to resolve the Cloud API of upstream %q: %w", up.Name, err)
	}

	// A single connection, whatever maxConnections says: translated calls are
	// short control-plane requests, not the long polls a pool spreads out.
	conn, err := connect.NewConn(p.Pool.Scoped("cloud_api/"+up.Name), res)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create the Cloud API client of upstream %q: %w", up.Name, err)
	}

	log := p.Logger.With(tag.String("upstream", up.Name), tag.String("hostPort", api.Listen.HostPort))
	if !p.Config.APITranslations.CloudAPI.IsSaasAPI() {
		// Nothing but a Cloud deployment serves CloudService, but a test double or
		// a private environment legitimately carries no Cloud domain, so this is
		// reported rather than rejected.
		log.Warn("the configured Cloud API is not a Temporal Cloud endpoint")
	}

	// An answered method never reaches the Cloud API, so it is reported apart from
	// the ones that do.
	var translated, answered []string
	for _, method := range reg.Methods() {
		if t, _ := reg.Lookup(method); t.To() == "" {
			answered = append(answered, method)
		} else {
			translated = append(translated, method)
		}
	}

	log.Info("translating methods to the Cloud API", tag.String("methods", strings.Join(translated, ", ")))
	if len(answered) > 0 {
		log.Info("answering methods Cloud cannot serve", tag.String("methods", strings.Join(answered, ", ")))
	}

	return reg, conn, nil
}
