package server

import (
	"context"
	"fmt"
	"net"

	"go.temporal.io/server/common/log"
	"go.uber.org/fx"

	"github.com/temporalio/temporal-proxy/internal/config"
)

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

type ServerParams struct {
	fx.In
	Lifecycle fx.Lifecycle

	// Required values
	Context context.Context
	Config  *config.Config

	// Optional values
	Credentials Credentials `optional:"true"`
	HealthCheck HealthCheck `optional:"true"`
	Logger      log.Logger  `optional:"true"`
}
