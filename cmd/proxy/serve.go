package main

import (
	"context"
	"os"

	"github.com/urfave/cli/v3"

	"go.temporal.io/proxy/internal/app"
	"go.temporal.io/proxy/internal/config"
	"go.temporal.io/proxy/pkg/logger"
	"go.temporal.io/proxy/pkg/logger/tag"
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
			&cli.BoolFlag{
				Name:    "require-encryption",
				Usage:   "Refuse to start unless the config has an encryption key (encryption.default)",
				Sources: cli.EnvVars("PROXY_REQUIRE_ENCRYPTION"),
			},
		},
		Action: func(ctx context.Context, cmd *cli.Command) error {
			log := logger.NewZeroLogger(os.Stderr, logger.ParseLevel(cmd.String("level")))

			fxApp := app.New(app.Inputs{
				Context:           ctx,
				Logger:            log,
				Config:            config.File(cmd.String("config")),
				RequireEncryption: cmd.Bool("require-encryption"),
			})
			if err := fxApp.Err(); err != nil {
				log.Error("Misconfigured fx app", tag.Error(err))
				return err
			}

			fxApp.Run()
			return nil
		},
	}
}
