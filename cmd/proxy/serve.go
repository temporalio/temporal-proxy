package main

import (
	"context"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/urfave/cli/v3"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.uber.org/fx"

	"github.com/temporalio/temporal-proxy/internal/config"
	"github.com/temporalio/temporal-proxy/internal/server"
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
				Sources:   cli.EnvVars("CODEC_SERVER_CONFIG"),
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
			logger := log.NewZapLogger(log.BuildZapLogger(log.Config{
				Level: cmd.String("level"),
			}))

			fxApp := fx.New(
				fx.Supply(
					prometheus.WrapRegistererWithPrefix("proxy_", prometheus.NewRegistry()),
					fx.Annotate(ctx, fx.As(new(context.Context))),
					fx.Annotate(cmd.String("config"), config.ConfigFileTag),
				),
				fx.Provide(
					func() prometheus.Gatherer { return prometheus.DefaultGatherer },
					func() log.Logger { return logger },
				),
				config.Module,
				server.Module,
				fx.NopLogger,
			)

			if err := fxApp.Err(); err != nil {
				logger.Error("Misconfigured fx app", tag.Error(err))
				return err
			}

			fxApp.Run()
			return nil
		},
	}
}
