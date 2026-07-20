package main

import (
	"context"
	"os"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/urfave/cli/v3"
	"go.uber.org/fx"

	"github.com/temporalio/temporal-proxy/internal/auth"
	"github.com/temporalio/temporal-proxy/internal/config"
	"github.com/temporalio/temporal-proxy/internal/metrics"
	"github.com/temporalio/temporal-proxy/internal/protoutil"
	"github.com/temporalio/temporal-proxy/internal/proxy"
	"github.com/temporalio/temporal-proxy/internal/router"
	"github.com/temporalio/temporal-proxy/internal/server"
	"github.com/temporalio/temporal-proxy/internal/transport/connect"
	"github.com/temporalio/temporal-proxy/pkg/logger"
	"github.com/temporalio/temporal-proxy/pkg/logger/tag"
)

func serve() *cli.Command {
	return &cli.Command{
		Name:  "serve",
		Usage: "Start the proxy server",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:      "config",
				Aliases:   []string{"c"},
				Usage:     "Path to the config file",
				TakesFile: true,
				Sources:   cli.EnvVars("PROXY_CONFIG"),
				Required:  true,
			},
			&cli.StringFlag{
				Name:    "level",
				Usage:   "Set the log level (debug, info, warn, error)",
				Value:   "info",
				Sources: cli.EnvVars("LOG_LEVEL"),
			},
			&cli.StringFlag{
				Name:    "metrics-addr",
				Usage:   "The host:port on which to serve /metrics",
				Value:   ":9090",
				Sources: cli.EnvVars("METRICS_ADDR"),
			},
			&cli.StringFlag{
				Name:    "metrics-namespace",
				Usage:   "The prometheus namespace for metrics",
				Value:   "tmprl_proxy",
				Sources: cli.EnvVars("METRICS_NAMESPACE"),
			},
		},
		Action: func(ctx context.Context, cmd *cli.Command) error {
			log := logger.NewZeroLogger(os.Stderr, logger.ParseLevel(cmd.String("level")))

			fxApp := fx.New(
				fx.Supply(
					fx.Annotate(ctx, fx.As(new(context.Context))),
					fx.Annotate(cmd.String("config"), config.ConfigFileTag),
					fx.Annotate(cmd.String("metrics-addr"), metrics.AddrTag),
					fx.Annotate(cmd.String("metrics-namespace"), metrics.NamespaceTag),
				),
				fx.Provide(
					func() logger.Logger { return log },
					func() prometheus.Gatherer { return prometheus.DefaultGatherer },
					func() prometheus.Registerer { return prometheus.DefaultRegisterer },
				),
				auth.Module,
				config.Module,
				connect.Module,
				metrics.Module,
				protoutil.Module,
				proxy.Module,
				router.Module,
				server.Module,
				fx.NopLogger,
			)

			if err := fxApp.Err(); err != nil {
				log.Error("Misconfigured fx app", tag.Error(err))
				return err
			}

			fxApp.Run()
			return nil
		},
	}
}
