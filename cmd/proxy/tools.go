package main

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"fmt"

	"github.com/urfave/cli/v3"
)

func tools() *cli.Command {
	return &cli.Command{
		Name:     "tools",
		Usage:    "Tools and utilities",
		Commands: []*cli.Command{generateKey()},
	}
}

func generateKey() *cli.Command {
	return &cli.Command{
		Name:  "gen-key",
		Usage: "Generates a random 32-byte, base64 encoded key",
		Action: func(ctx context.Context, cmd *cli.Command) error {
			var key [32]byte

			_, err := rand.Read(key[:])
			if err != nil {
				return err
			}

			_, err = fmt.Fprintf(
				cmd.Writer,
				"Generated a new key:\nbase64key://%s\n",
				base64.URLEncoding.EncodeToString(key[:]),
			)
			return err
		},
	}
}
