package main

import (
	"context"
	"fmt"
	"net"
	"os"
	"time"

	"github.com/urfave/cli/v3"
	"go.temporal.io/server/common/log"
	"go.uber.org/fx"

	"github.com/temporalio/temporal-proxy/internal/server"
)

// NB: These are set at build time by the CI/CD process.
var (
	buildTime string = time.Now().UTC().Format(time.RFC3339)
	sha       string = "unknown"
	version   string = "local"
)

func main() {
	app := &cli.Command{
		Name:    "proxy",
		Usage:   "A universal proxy for Temporal",
		Version: version,
		Action: func(ctx context.Context, cmd *cli.Command) error {
			fxApp := fx.New(
				fx.Supply(
					fx.Annotate(ctx, fx.As(new(context.Context))),
				),
				fx.Provide(
					func() log.Logger {
						return log.NewZapLogger(log.BuildZapLogger(log.Config{
							// TODO: Make a real config
						}))
					},
					func() (net.Listener, error) {
						return (&net.ListenConfig{}).Listen(ctx, "tcp", ":0")
					},
				),
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

	// nolint:errcheck // TODO: disable this for fmt calls in golangci.yaml
	cli.VersionPrinter = func(cmd *cli.Command) {
		fmt.Fprintln(cmd.Writer, cmd.Name, "-", cmd.Usage)
		fmt.Fprintln(cmd.Writer, "Version:", version)
		fmt.Fprintln(cmd.Writer, "Built At:", buildTime)
		fmt.Fprintln(cmd.Writer, "Git SHA:", sha)
	}

	ctx := context.Background()
	if err := app.Run(ctx, os.Args); err != nil {
		os.Exit(1)
	}
}
