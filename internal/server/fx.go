package server

import (
	"context"
	"fmt"
	"net"

	"go.uber.org/fx"

	"github.com/temporalio/temporal-proxy/internal/config"
	"github.com/temporalio/temporal-proxy/internal/transport/creds"
	"github.com/temporalio/temporal-proxy/pkg/logger"
)

// Module is the fx module that constructs a [Server] from [ServerParams] and
// binds its lifecycle to the application.
var Module = fx.Option(fx.Invoke(func(p ServerParams) error {
	if err := p.Config.Validate(); err != nil {
		return fmt.Errorf("invalid configuration: %w", err)
	}

	opts := make([]Option, 0, 3)
	opts = append(opts, WithCredentials(p.creds()))

	if p.Logger != nil {
		opts = append(opts, WithLogger(p.Logger))
	}

	if p.HealthCheck != nil {
		opts = append(opts, WithHealthCheck(p.HealthCheck))
	}

	svr, err := New(opts...)
	if err != nil {
		return err
	}

	p.Lifecycle.Append(fx.Hook{
		OnStart: func(context.Context) error {
			lis, err := (&net.ListenConfig{}).Listen(
				p.Context,
				"tcp",
				p.Config.Listen.HostPort,
			)
			if err != nil {
				return fmt.Errorf("failed to create listener: %w", err)
			}

			go func() {
				defer func() { _ = lis.Close() }()

				if err := svr.Start(p.Context, lis); err != nil {
					// The server stopped serving unexpectedly. Bring the app
					// down rather than linger in a non-serving state; Start has
					// already logged the cause.
					_ = p.Shutdowner.Shutdown(fx.ExitCode(1))
				}
			}()

			return nil
		},
		OnStop: func(ctx context.Context) error {
			return svr.Stop(ctx)
		},
	})

	return nil
}))

// ServerParams collects the fx-provided dependencies needed to construct a
// [Server]. Context and Config are required; HealthCheck and Logger are
// optional and fall back to the defaults used by [New] when not supplied. The
// listen address and transport credentials are derived from Config.
type ServerParams struct {
	fx.In
	Lifecycle  fx.Lifecycle
	Shutdowner fx.Shutdowner

	// Required values
	Context context.Context
	Config  *config.Config

	// Optional values
	HealthCheck HealthCheck   `optional:"true"`
	Logger      logger.Logger `optional:"true"`
}

func (p *ServerParams) creds() Credentials {
	tls := p.Config.Listen.TLS
	if tls == nil {
		return creds.NewInsecure()
	}

	if tls.CA != "" {
		return creds.NewMTLS(
			tls.CA,
			tls.Cert,
			tls.Key,
			creds.MTLSOptions{
				ServerName: tls.ServerName,
			},
		)
	}

	return creds.NewServerTLS(tls.Cert, tls.Key)
}
