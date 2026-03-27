package server

import (
	"context"
	"net"

	"go.temporal.io/server/common/log"
	"go.uber.org/fx"
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
			go svr.Start(p.Context, p.Listener) // nolint:errcheck
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
	Context  context.Context
	Listener net.Listener

	// Optional values
	Credentials Credentials `optional:"true"`
	HealthCheck HealthCheck `optional:"true"`
	Logger      log.Logger  `optional:"true"`
}
