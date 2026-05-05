package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/urfave/cli/v3"
	"go.temporal.io/server/common/log"
	"go.uber.org/fx"

	"github.com/temporalio/temporal-proxy/internal/adapt"
	"github.com/temporalio/temporal-proxy/internal/config"
	"github.com/temporalio/temporal-proxy/internal/crypto"
	"github.com/temporalio/temporal-proxy/internal/server"
)

var (
	// NB: These are set at build time by the CI/CD process.
	buildTime string = time.Now().UTC().Format(time.RFC3339)
	sha       string = "unknown"
	version   string = "local"

	configFlag = &cli.StringFlag{
		Name:      "config",
		Aliases:   []string{"c"},
		Usage:     "Path to the config file",
		TakesFile: true,
		Sources:   cli.EnvVars("TEMPORAL_PROXY_CONFIG"),
		Required:  true,
	}
)

func main() {
	// nolint:errcheck // TODO: disable this for fmt calls in golangci.yaml
	cli.VersionPrinter = func(cmd *cli.Command) {
		fmt.Fprintln(cmd.Writer, cmd.Name, "-", cmd.Usage)
		fmt.Fprintln(cmd.Writer, "Version:", version)
		fmt.Fprintln(cmd.Writer, "Built At:", buildTime)
		fmt.Fprintln(cmd.Writer, "Git SHA:", sha)
	}

	app := &cli.Command{
		Name:    "proxy",
		Usage:   "A universal proxy for Temporal",
		Version: version,
		Commands: []*cli.Command{
			run(),
			tools(),
		},
	}

	ctx := context.Background()
	if err := app.Run(ctx, os.Args); err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		os.Exit(1)
	}
}

func run() *cli.Command {
	return &cli.Command{
		Name:  "run",
		Usage: "Run the proxy",
		Flags: []cli.Flag{configFlag},
		Action: func(ctx context.Context, cmd *cli.Command) error {
			fxApp := fx.New(
				fx.Supply(
					fx.Annotate(ctx, fx.As(new(context.Context))),
					fx.Annotate(cmd.String("config"), fx.ResultTags(`name:"configFile"`)),
				),
				fx.Provide(
					func() log.Logger {
						return log.NewZapLogger(log.BuildZapLogger(log.Config{
							// TODO: Make a real config
						}))
					},
				),
				adapt.Module,
				config.Module,
				crypto.Module,
				server.Module,
				fx.NopLogger,
			)

			if err := fxApp.Err(); err != nil {
				return err
			}

			fxApp.Run()
			return nil
		},
	}
}
