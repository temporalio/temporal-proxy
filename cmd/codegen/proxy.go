package main

import (
	"context"
	"fmt"
	"reflect"
	"strings"

	"github.com/urfave/cli/v3"
	"go.temporal.io/api/workflowservice/v1"

	"github.com/temporalio/temporal-proxy/internal/protoutil"
)

var serviceMap = map[string]service{
	"WorkflowService": {
		Name:       "WorkflowService",
		ImportPath: "go.temporal.io/api/workflowservice/v1",
		Type:       reflect.TypeFor[workflowservice.WorkflowServiceServer](),
	},
}

type (
	templateData struct {
		Package    string
		ImportPath string
		Service    *protoutil.Service
	}

	service struct {
		Name       string
		ImportPath string
		Type       reflect.Type
	}
)

func proxyCommand() *cli.Command {
	return &cli.Command{
		Name:  "proxy",
		Usage: "Generates a service implementation that proxies to a client",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:     "out",
				Aliases:  []string{"o"},
				Usage:    "Where to write the output file(s)",
				Required: true,
			},
			&cli.StringFlag{
				Name:     "type",
				Aliases:  []string{"t"},
				Usage:    "The name of the service",
				Required: true,
			},
		},
		Action: func(ctx context.Context, cmd *cli.Command) error {
			svc, ok := serviceMap[cmd.String("type")]
			if !ok {
				return fmt.Errorf("unknown type: %s", cmd.String("type"))
			}

			s, err := protoutil.ParseService(svc.Type)
			if err != nil {
				return err
			}

			s.Name = svc.Name // name override for templating
			data := &templateData{
				Package:    "proxy",
				ImportPath: svc.ImportPath,
				Service:    s,
			}

			for tmpl, path := range map[string]string{
				"service":      cmd.String("out"),
				"service_test": strings.TrimSuffix(cmd.String("out"), ".go") + "_test.go",
			} {
				if err := writeFile(tmpl, path, data); err != nil {
					return err
				}
			}

			return nil
		},
	}
}
