package main

import (
	"context"
	"fmt"
	"os"

	"github.com/urfave/cli/v3"

	"github.com/temporalio/temporal-proxy/internal/version"
)

func main() {
	// nolint:errcheck // TODO: disable this for fmt calls in golangci.yaml
	cli.VersionPrinter = func(cmd *cli.Command) {
		fmt.Fprintln(cmd.Writer, cmd.Name, "-", cmd.Usage)
		fmt.Fprintln(cmd.Writer, "Version:", version.Version)
		fmt.Fprintln(cmd.Writer, "Built At:", version.BuildTime)
		fmt.Fprintln(cmd.Writer, "Git SHA:", version.SHA)
	}

	app := &cli.Command{
		Name:    "proxy",
		Usage:   "The official Temporal proxy server",
		Version: version.Version,
		Commands: []*cli.Command{
			serve(),
		},
	}

	ctx := context.Background()
	if err := app.Run(ctx, os.Args); err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		os.Exit(1)
	}
}
