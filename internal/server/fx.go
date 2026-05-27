package server

import (
	"context"
	"fmt"
	"net"

	"go.temporal.io/server/common/log"
	"go.uber.org/fx"
)

// DefaultHostPort is the TCP bind address used when [ServerParams.HostPort]
// is empty.
const DefaultHostPort = ":8443"

// Module is the fx module that constructs a [Server] from [ServerParams] and
// binds its lifecycle to the application.
var Module = fx.Option(fx.Invoke(func(p ServerParams) error {
	opts := make([]Option, 0, 3)
	if p.Logger != nil {
		opts = append(opts, WithLogger(p.Logger))
	}

	if p.Credentials != nil {
		opts = append(opts, WithCredentials(p.Credentials))
	}

	if p.HealthCheck != nil {
		opts = append(opts, WithHealthCheck(p.HealthCheck))
	}

	svr, err := New(opts...)
	if err != nil {
		return err
	}

	hostPort := p.HostPort
	if hostPort == "" {
		hostPort = DefaultHostPort
	}

	p.Lifecycle.Append(fx.Hook{
		OnStart: func(context.Context) error {
			lis, err := (&net.ListenConfig{}).Listen(
				p.Context,
				"tcp",
				hostPort,
			)
			if err != nil {
				return fmt.Errorf("failed to create listener: %w", err)
			}

			go func() {
				defer func() { _ = lis.Close() }()

				svr.Start(p.Context, lis) // nolint:errcheck
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
// [Server]. HostPort, Credentials, HealthCheck, and Logger are optional and
// fall back to the defaults used by [New] (or [DefaultHostPort]) when not
// supplied. HostPort must be provided as a named string value tagged
// "serverHostPort".
type ServerParams struct {
	fx.In
	Lifecycle fx.Lifecycle

	// Required values
	Context context.Context

	// Optional values
	HostPort    string      `name:"serverHostPort" optional:"true"`
	Credentials Credentials `optional:"true"`
	HealthCheck HealthCheck `optional:"true"`
	Logger      log.Logger  `optional:"true"`
}
