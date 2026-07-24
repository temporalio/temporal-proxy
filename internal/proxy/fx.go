package proxy

import (
	"context"
	"fmt"
	"slices"

	"go.uber.org/fx"
	"google.golang.org/grpc"

	"github.com/temporalio/temporal-proxy/internal/auth"
	"github.com/temporalio/temporal-proxy/internal/config"
	"github.com/temporalio/temporal-proxy/internal/protoutil"
	"github.com/temporalio/temporal-proxy/internal/transport/connect"
	"github.com/temporalio/temporal-proxy/internal/transport/creds"
	"github.com/temporalio/temporal-proxy/pkg/logger"
)

// Module is the fx module that constructs the proxy [Server] from [ProxyParams]
// and binds its lifecycle to the application.
var Module = fx.Options(fx.Invoke(func(p ProxyParams) error {
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

		cp, err := auth.CredentialProviderFor(up.Credentials)
		if err != nil {
			return fmt.Errorf("invalid credentials for upstream %q: %w", up.Name, err)
		}
		if cp != nil {
			dialOpts = append(dialOpts, auth.DialOptions(cp)...)
		}

		res, err := upstreamResolver(up, dialOpts)
		if err != nil {
			return err
		}

		conn, err := connect.NewConn(p.Pool.ConnOrCreate, res)
		if err != nil {
			return err
		}

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

	// Optional values
	Logger logger.Logger `optional:"true"`
}

// upstreamResolver builds the [connect.Resolver] for an upstream. When neither
// the hostPort nor the TLS server name is templated it returns a static
// resolver whose connection is constructed eagerly (and reused for every
// request); otherwise it returns a DynamicResolver that renders the target and
// server name, and rebuilds credentials, per request. opts holds the
// request-independent dial options (namespace translation and outbound
// credentials).
func upstreamResolver(upstream *config.Upstream, opts []grpc.DialOption) (connect.Resolver, error) {
	if upstream.IsTemplated() {
		translator := func(s string) string { return s }
		if upstream.Namespaces.Rules.Configured() {
			translator = upstream.Namespaces.Rules.Remote
		}

		// Share one loader across the per-request credentials so a templated
		// upstream reads and parses its TLS material once rather than on every
		// request. Mutual TLS (a configured client certificate) uses a CertLoader
		// for the client key pair and CA; CA-only client TLS uses a CAPoolLoader
		// for the trust anchor. The system-root client-TLS and insecure paths have
		// no files to load, so both loaders stay nil.
		var (
			loader   *creds.CertLoader
			caLoader *creds.CAPoolLoader
		)
		if tls := upstream.Listen.TLS; tls != nil {
			switch {
			case tls.Cert != "" || tls.Key != "":
				loader = creds.NewCertLoader(tls.CA, tls.Cert, tls.Key)
			case tls.CA != "":
				caLoader = creds.NewCAPoolLoader(tls.CA)
			}
		}

		return NewDynamicResolver(
			upstream,
			WithRemoteNamespacer(translator),
			WithOptionsFactory(func(data RouteData) ([]grpc.DialOption, error) {
				creds, err := upstreamCreds(upstream, data.ResolvedServerName, loader, caLoader).DialOption()
				if err != nil {
					return nil, err
				}

				return append(slices.Clone(opts), creds), nil
			}),
		)
	}

	serverName := ""
	if upstream.Listen.TLS != nil {
		serverName = upstream.Listen.TLS.ServerName
	}

	creds, err := upstreamCreds(upstream, serverName, nil, nil).DialOption()
	if err != nil {
		return nil, fmt.Errorf("failed to build credentials for upstream %q: %w", upstream.Name, err)
	}

	return connect.StaticResolver(upstream.Listen.HostPort, append(slices.Clone(opts), creds)...), nil
}

// upstreamCreds derives the credentials used to dial the upstream frontend from
// the upstream TLS configuration. A configured client certificate (cert+key)
// selects mutual TLS, which verifies the upstream against the configured CA.
// Without a client certificate the proxy uses client-side TLS: a custom root
// pool when a CA is set, otherwise the system root pool. No TLS at all is
// insecure. serverName overrides the SNI/certificate-verification name; it is
// the upstream's static configured ServerName, or a per-request value rendered
// from a templated ServerName. loader and caLoader, when non-nil, supply the
// pre-parsed TLS material (mutual-TLS key pair plus CA, and CA-only trust
// anchor respectively) so it is loaded once and reused across the per-request
// credentials of a templated upstream; pass nil for both on a fixed-address
// upstream. At most one is used, selected by the same TLS mode as the returned
// credential.
func upstreamCreds(upstream *config.Upstream, serverName string, loader *creds.CertLoader, caLoader *creds.CAPoolLoader) Credentials {
	tls := upstream.Listen.TLS
	if tls == nil {
		return creds.NewInsecure()
	}

	if tls.Cert != "" || tls.Key != "" {
		return creds.NewMTLS(tls.CA, tls.Cert, tls.Key, creds.MTLSOptions{
			ServerName: serverName,
			Loader:     loader,
		})
	}

	if tls.CA != "" {
		return creds.NewClientTLSWithCA(tls.CA, serverName, caLoader)
	}

	return creds.NewClientTLS(serverName)
}
