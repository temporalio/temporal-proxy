package dataplane

import (
	"context"
	"fmt"
	"slices"
	"strings"

	"go.uber.org/fx"
	"google.golang.org/grpc"

	"go.temporal.io/proxy/internal/auth/outbound"
	"go.temporal.io/proxy/internal/cloud/translation"
	"go.temporal.io/proxy/internal/config"
	"go.temporal.io/proxy/internal/protoutil"
	"go.temporal.io/proxy/internal/proxy"
	"go.temporal.io/proxy/internal/transport/connect"
	"go.temporal.io/proxy/internal/transport/creds"
	"go.temporal.io/proxy/internal/version"
	"go.temporal.io/proxy/pkg/logger"
	"go.temporal.io/proxy/pkg/logger/tag"
)

type (
	// upstreams is every configured upstream, built: the handler the gateway
	// routes each one's streams to, by upstream name, and the connections opened
	// before it serves.
	upstreams struct {
		handlers map[string]grpc.StreamHandler

		// ready holds only static upstreams' connections; a templated upstream
		// resolves its target per request and has nothing to open yet.
		ready []*connect.Conn
	}

	// upstreamParams collects what every upstream is built with. Reloader may be
	// nil, which leaves every credential with the material it loaded first.
	upstreamParams struct {
		fx.In
		Lifecycle fx.Lifecycle

		Config     *config.Config
		Translator *protoutil.Translator
		Types      protoutil.Types
		Pool       *connect.Pool
		Codecs     *proxy.Codecs
		Limiter    *proxy.DecodeLimiter
		Reloader   *creds.Reloader
		Logger     logger.Logger
	}
)

// newUpstreams builds a forwarder for every configured upstream. It creates each
// connection here and opens the static ones when the application starts. gRPC
// does not open a socket until a connection is used, so opening them is what
// makes an unreachable upstream fail startup. The gateway is built from these
// upstreams, so its start hook is appended after this one and binds only once
// they are open.
func newUpstreams(p upstreamParams) (*upstreams, error) {
	cfg := p.Config

	// A translation block nothing will consult changes nothing and would leave an
	// operator waiting for behaviour that cannot arrive, so say so rather than
	// ignoring it silently.
	if !cfg.APITranslations.CloudAPI.IsZero() && !translates(cfg) {
		p.Logger.Warn(
			"apiTranslations is configured but no upstream is Temporal Cloud, so no method will be translated",
		)
	}

	out := &upstreams{handlers: make(map[string]grpc.StreamHandler, len(cfg.Upstreams))}
	for i := range cfg.Upstreams {
		up := &cfg.Upstreams[i]

		fw, ready, err := newUpstreamForwarder(&p, up)
		if err != nil {
			return nil, err
		}

		if ready != nil {
			out.ready = append(out.ready, ready)
		}

		// The gateway calls this upstream's forwarder directly with the stream it
		// accepted.
		out.handlers[up.Name] = fw.Handle
	}

	p.Lifecycle.Append(fx.StartHook(func(ctx context.Context) error {
		if err := connect.WaitReady(ctx, out.ready...); err != nil {
			return fmt.Errorf("upstream connection not ready: %w", err)
		}

		return nil
	}))

	return out, nil
}

// newUpstreamForwarder builds one upstream's forwarder. It returns the
// forwarder, the connection to open eagerly at start (nil for a templated
// upstream, which resolves per request and has nothing to open yet), and any
// error.
func newUpstreamForwarder(p *upstreamParams, up *config.Upstream) (*proxy.Forwarder, *connect.Conn, error) {
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

	cdc, err := p.Codecs.Interceptor(codecOpts...)
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
		proxy.WithDecodeBudget(p.Limiter, int64(up.Connection.ResponseLimit())),
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
func cloudAPIConn(p *upstreamParams, up *config.Upstream) (*translation.Registry, *connect.Conn, error) {
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
