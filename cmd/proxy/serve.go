package main

import (
	"context"
	"os"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/urfave/cli/v3"
	"go.uber.org/fx"

	"github.com/temporalio/temporal-proxy/internal/config"
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
		},
		Action: func(ctx context.Context, cmd *cli.Command) error {
			log := logger.NewZeroLogger(os.Stderr, logger.ParseLevel(cmd.String("level")))

			fxApp := fx.New(
				fx.Supply(
					prometheus.WrapRegistererWithPrefix("proxy_", prometheus.NewRegistry()),
					fx.Annotate(ctx, fx.As(new(context.Context))),
					fx.Annotate(cmd.String("config"), config.ConfigFileTag),
				),
				fx.Provide(
					func() prometheus.Gatherer { return prometheus.DefaultGatherer },
					func() logger.Logger { return log },
				),
				config.Module,
				connect.Module,
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
