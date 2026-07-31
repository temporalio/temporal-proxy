package proxy

import (
	"context"
	"fmt"
	"slices"

	"go.uber.org/fx"
	"google.golang.org/grpc"

	"github.com/temporalio/temporal-proxy/internal/auth/outbound"
	"github.com/temporalio/temporal-proxy/internal/config"
	"github.com/temporalio/temporal-proxy/internal/metrics"
	"github.com/temporalio/temporal-proxy/internal/protoutil"
	"github.com/temporalio/temporal-proxy/internal/transport/connect"
	"github.com/temporalio/temporal-proxy/pkg/crypto"
	"github.com/temporalio/temporal-proxy/pkg/logger"
)

// Module is the fx module that constructs the proxy [Server] from [ProxyParams]
// and binds its lifecycle to the application.
var Module = fx.Options(fx.Invoke(func(p ProxyParams) error {
	// Encryption is a security control: if it is enabled but no vault reached
	// the proxy (a wiring fault, since kms.Module builds one whenever keys are
	// configured), fail fast rather than silently forwarding cleartext upstream.
	if p.Config.Encryption.Enabled && p.Vault == nil {
		return fmt.Errorf("encryption is enabled but no vault was provided")
	}

	// Built once (not per upstream) so its collectors register with Prometheus
	// exactly once; a per-upstream build would panic on duplicate registration.
	var encReporter *Reporter
	if p.Vault != nil {
		encReporter = NewReporter(p.Factory.ForSubsystem("encryption"))
	}

	conns := make([]*connect.Conn, 0, len(p.Config.Upstreams))

	for i := range p.Config.Upstreams {
		up := &p.Config.Upstreams[i]
		if err := up.Validate(); err != nil {
			return fmt.Errorf("invalid upstream configuration: %w", err)
		}

		// Request-independent dial options: namespace translation and outbound
		// credentials. Per-request credentials are added by the resolver.
		var dialOpts []grpc.DialOption
		rules := &up.Namespaces.Rules
		if rules.Configured() {
			dialOpts = append(dialOpts, translationDialOptions(p.Translator, rules.Remote, rules.Local)...)
		}

		cp, err := outbound.CredentialProviderFor(up.Credentials)
		if err != nil {
			return fmt.Errorf("invalid credentials for upstream %q: %w", up.Name, err)
		}
		if cp != nil {
			dialOpts = append(dialOpts, outbound.DialOptions(cp)...)
		}

		// Payload encryption. A vault is present whenever encryption keys are
		// configured (see kms.Module), which may be true even when encryption is
		// disabled; install the interceptor whenever one is present and pass
		// Enabled so sealing is gated while inbound decryption always runs. This
		// keeps payloads sealed earlier openable after encryption is turned off
		// for new traffic. Added after translation so it is the innermost unary
		// interceptor, sealing outbound payloads last and opening inbound
		// payloads first.
		if p.Vault != nil {
			enc, err := EncryptionInterceptor(p.Config.Encryption.Enabled, p.Vault, encReporter)
			if err != nil {
				return fmt.Errorf("failed to build encryption interceptor for upstream %q: %w", up.Name, err)
			}

			dialOpts = append(dialOpts, grpc.WithChainUnaryInterceptor(enc))
		}

		res, err := upstreamResolver(up, dialOpts)
		if err != nil {
			return err
		}

		conn, err := connect.NewConn(p.Pool.ConnOrCreate, res)
		if err != nil {
			return err
		}

		conns = append(conns, conn)

		var opts []Option
		if p.Logger != nil {
			opts = append(opts, WithLogger(p.Logger))
		}

		svr, err := New(up.Listen.HostPort, conn, opts...)
		if err != nil {
			return fmt.Errorf("failed to create proxy for upstream %q: %w", up.Name, err)
		}

		p.Lifecycle.Append(fx.Hook{
			OnStart: func(context.Context) error {
				// Bind synchronously so the socket is listening before the
				// inbound server (whose OnStart runs after this one) starts
				// routing requests to it; then serve in the background.
				lis, err := svr.Listen(p.Context)
				if err != nil {
					return fmt.Errorf("failed to start proxy for upstream %q: %w", up.Name, err)
				}

				go func() {
					defer func() { _ = lis.Close() }()

					if err := svr.Start(p.Context, lis); err != nil {
						// The proxy stopped serving unexpectedly. Bring the app
						// down rather than linger in a non-serving state; Start
						// has already logged the cause.
						_ = p.Shutdowner.Shutdown(fx.ExitCode(1))
					}
				}()

				return nil
			},
			OnStop: svr.Stop,
		})
	}

	// A static upstream's connection is created while the graph is built, but gRPC
	// does not open a socket until it is used, so open them here: an unreachable
	// upstream fails startup instead of surfacing as request errors once the proxy
	// is already serving. Templated upstreams resolve their target per request and
	// have nothing to open yet, so they are skipped (see [connect.Conn.WaitReady])
	// and stay unverified until traffic arrives.
	p.Lifecycle.Append(fx.StartHook(func(ctx context.Context) error {
		if err := connect.WaitReady(ctx, conns...); err != nil {
			return fmt.Errorf("upstream connection not ready: %w", err)
		}

		return nil
	}))

	return nil
}))

// ProxyParams collects the fx-provided dependencies needed to construct and run
// the proxy [Server]. Context, Config, Translator, and Pool are required;
// Logger is optional and falls back to the default used by [New] when not
// supplied. [protoutil.Module] provides the Translator and [connect.Module]
// provides the Pool in the assembled application.
type ProxyParams struct {
	fx.In
	Lifecycle  fx.Lifecycle
	Shutdowner fx.Shutdowner

	// Required values
	Context    context.Context
	Config     *config.Config
	Translator *protoutil.Translator
	Pool       *connect.Pool
	Vault      *crypto.Vault
	Factory    *metrics.Factory

	// Optional values
	Logger logger.Logger `optional:"true"`
}

// upstreamResolver builds the [connect.Resolver] for an upstream. When neither
// the hostPort nor the TLS server name is templated it returns a static
// resolver, whose connection is constructed while the graph is built, opened on
// start, and reused for every request; otherwise it returns a DynamicResolver
// that renders the target and server name, and rebuilds credentials, per request.
// opts holds the request-independent dial options (namespace translation and
// outbound credentials).
func upstreamResolver(upstream *config.Upstream, opts []grpc.DialOption) (connect.Resolver, error) {
	// One Dialer per upstream owns the TLS-mode decision and parses its
	// certificate material once, so a templated upstream reuses it across every
	// per-request dial (only the rendered server name varies).
	dialer := upstream.Listen.TLS.Dialer()

	if upstream.IsTemplated() {
		translator := func(s string) string { return s }
		if upstream.Namespaces.Rules.Configured() {
			translator = upstream.Namespaces.Rules.Remote
		}

		return NewDynamicResolver(
			upstream,
			WithRemoteNamespacer(translator),
			WithOptionsFactory(func(data RouteData) ([]grpc.DialOption, error) {
				cred, err := dialer.DialOption(data.ResolvedServerName)
				if err != nil {
					return nil, err
				}

				return append(slices.Clone(opts), cred), nil
			}),
		)
	}

	serverName := ""
	if upstream.Listen.TLS != nil {
		serverName = upstream.Listen.TLS.ServerName
	}

	cred, err := dialer.DialOption(serverName)
	if err != nil {
		return nil, fmt.Errorf("failed to build credentials for upstream %q: %w", upstream.Name, err)
	}

	return connect.StaticResolver(upstream.Listen.HostPort, append(slices.Clone(opts), cred)...), nil
}
