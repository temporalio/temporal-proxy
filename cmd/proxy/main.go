package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/urfave/cli/v3"
)

// NB: These are set at build time by the CI/CD process.
var (
	buildTime string = time.Now().UTC().Format(time.RFC3339)
	sha       string = "unknown"
	version   string = "local"
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
		Usage:   "The official Temporal proxy server",
		Version: version,
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
