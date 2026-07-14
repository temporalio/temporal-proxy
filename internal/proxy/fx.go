package proxy

import (
	"context"
	"fmt"

	"go.uber.org/fx"

	"github.com/temporalio/temporal-proxy/internal/config"
	"github.com/temporalio/temporal-proxy/internal/transport/creds"
	"github.com/temporalio/temporal-proxy/pkg/logger"
)

// Module is the fx module that constructs the proxy [Server] from [ProxyParams]
// and binds its lifecycle to the application.
var Module = fx.Options(fx.Invoke(func(p ProxyParams) error {
	for i := range p.Config.Upstreams {
		upstream := &p.Config.Upstreams[i]

		if err := upstream.Validate(); err != nil {
			return fmt.Errorf("invalid upstream configuration: %w", err)
		}

		if upstream.IsTemplated() {
			return fmt.Errorf(
				"upstream %q has a templated hostPort %q: templated upstreams are not yet supported",
				upstream.Name,
				upstream.Listen.HostPort,
			)
		}

		opts := []Option{WithCredentials(upstreamCreds(upstream))}
		if p.Logger != nil {
			opts = append(opts, WithLogger(p.Logger))
		}

		svr, err := New(upstream.Listen.HostPort, opts...)
		if err != nil {
			return fmt.Errorf("failed to create proxy for upstream %q: %w", upstream.Name, err)
		}

		p.Lifecycle.Append(fx.Hook{
			OnStart: func(context.Context) error {
				go func() {
					if err := svr.Start(p.Context); err != nil {
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
// the proxy [Server]. Context and Config are required; Logger is optional and
// falls back to the default used by [New] when not supplied.
type ProxyParams struct {
	fx.In
	Lifecycle  fx.Lifecycle
	Shutdowner fx.Shutdowner

	// Required values
	Context context.Context
	Config  *config.Config

	// Optional values
	Logger logger.Logger `optional:"true"`
}

// upstreamCreds derives the credentials used to dial the upstream frontend from
// the upstream TLS configuration: mutual TLS when a CA is set, server-verified
// client TLS when TLS is configured without one, and insecure otherwise.
func upstreamCreds(upstream *config.Upstream) Credentials {
	tls := upstream.Listen.TLS
	if tls == nil {
		return creds.NewInsecure()
	}

	if tls.CA != "" {
		return creds.NewMTLS(
			tls.CA,
			tls.Cert,
			tls.Key,
			creds.MTLSOptions{ServerName: tls.ServerName},
		)
	}

	return creds.NewClientTLS()
}
