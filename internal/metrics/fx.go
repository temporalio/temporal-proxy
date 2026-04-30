package metrics

import (
	"context"
	"net"

	"github.com/prometheus/client_golang/prometheus"
	"go.temporal.io/server/common/log"
	"go.uber.org/fx"

	"github.com/temporalio/temporal-proxy/internal/config"
)

// Module wires the metrics Provider and optional HTTP exposition server into an fx application.
var Module = fx.Options(
	fx.Provide(func(p ProviderParams) *Provider {
		opts := []ProviderOption{Registerer(p.Registerer)}
		if p.Logger != nil {
			opts = append(opts, Logger(p.Logger))
		}
		return NewProvider(opts...)
	}),
	fx.Invoke(func(p ServerParams) error {
		// No server unless it's configured.
		if p.Config.Metrics.HostPort == "" {
			return nil
		}

		logger := p.Logger
		if logger == nil {
			logger = log.NewNoopLogger()
		}

		opts := make([]ServerOption, 0, 3)
		opts = append(opts, ServerLogger(logger))
		opts = append(opts, Gatherer(p.Gatherer))
		if p.Config.Metrics.Path != "" {
			opts = append(opts, Path(p.Config.Metrics.Path))
		}

		svr := NewServer(opts...)
		p.Lifecycle.Append(fx.Hook{
			OnStart: func(ctx context.Context) error {
				lis, err := (&net.ListenConfig{}).Listen(
					ctx,
					"tcp",
					p.Config.Metrics.HostPort,
				)
				if err != nil {
					return err
				}

				svr.Start(lis)
				return nil
			},
			OnStop: svr.Stop,
		})

		return nil
	}),
)

type (
	// ProviderParams holds the fx-injected dependencies for constructing a Provider.
	ProviderParams struct {
		fx.In
		Logger     log.Logger `optional:"true"`
		Registerer prometheus.Registerer
	}

	// ServerParams holds the fx-injected dependencies for starting the metrics HTTP server.
	ServerParams struct {
		fx.In
		Lifecycle fx.Lifecycle
		Context   context.Context
		Gatherer  prometheus.Gatherer
		Config    *config.Config
		Logger    log.Logger `optional:"true"`
	}
)
