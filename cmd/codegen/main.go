package main

import (
	"context"
	"log"
	"os"

	"github.com/urfave/cli/v3"
)

func main() {
	app := &cli.Command{
		Name:  "codegen",
		Usage: "A custom tool for generating code for the proxy",
		Commands: []*cli.Command{
			proxyCommand(),
		},
	}

	if err := app.Run(context.Background(), os.Args); err != nil {
		log.Fatalf("codegen error: %v", err)
	}
}
