package main

import (
	"bytes"
	"context"
	"embed"
	"fmt"
	"go/format"
	"io"
	"os"
	"reflect"
	"strings"
	"text/template"

	"github.com/urfave/cli/v3"
	"go.temporal.io/api/workflowservice/v1"
)

var (
	contextType = reflect.TypeFor[context.Context]()
	errorType   = reflect.TypeFor[error]()

	//go:embed *.go.tmpl
	templates embed.FS

	renderer = template.Must(template.New("").Funcs(template.FuncMap{
		"lower": strings.ToLower,
	}).ParseFS(templates, "*.go.tmpl"))

	serviceMap = map[string]service{
		"WorkflowService": {
			Name:       "WorkflowService",
			ImportPath: "go.temporal.io/api/workflowservice/v1",
			Type:       reflect.TypeFor[workflowservice.WorkflowServiceServer](),
		},
	}
)

type (
	templateData struct {
		Package string
		Service service
		Methods []method
	}

	service struct {
		Name       string
		ImportPath string
		Type       reflect.Type
	}

	method struct {
		Name string
		Req  string
		Resp string
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

			methods, err := collectMethods(svc)
			if err != nil {
				return err
			}

			data := &templateData{
				Package: "proxy",
				Service: svc,
				Methods: methods,
			}

			writeFile := func(tmpl, path string) error {
				var buf bytes.Buffer
				if err := render(&buf, tmpl, data); err != nil {
					return err
				}

				src, err := format.Source(buf.Bytes())
				if err != nil {
					return err
				}

				if err := os.WriteFile(path, src, 0o644); err != nil {
					return err
				}

				if tmpl == "service" {
					_, err = fmt.Fprintf(cmd.Writer, "codegen: wrote %d methods to %s\n", len(methods), path)
					return err
				}

				_, err = fmt.Fprintf(cmd.Writer, "codegen: wrote %s\n", path)
				return err
			}

			for tmpl, path := range map[string]string{
				"service":      cmd.String("out"),
				"service_test": strings.TrimSuffix(cmd.String("out"), ".go") + "_test.go",
			} {
				if err := writeFile(tmpl, path); err != nil {
					return err
				}
			}

			return nil
		},
	}
}

func render(w io.Writer, tmpl string, data *templateData) error {
	if err := renderer.ExecuteTemplate(w, tmpl+".go.tmpl", data); err != nil {
		return fmt.Errorf("failed to exec template: %s, %w", tmpl, err)
	}

	return nil
}

func collectMethods(svc service) ([]method, error) {
	methods := make([]method, 0, svc.Type.NumMethod())
	for m := range svc.Type.Methods() {
		ft := m.Type // interface method type carries no receiver

		if !isUnaryRPC(ft) {
			// Skips things like mustEmbedUnimplementedWorkflowServiceServer and would skip
			// any future streaming RPC (which the unary forward helper cannot
			// express) rather than emit code that does not compile.
			continue
		}

		req := ft.In(1).Elem()
		resp := ft.Out(0).Elem()
		for _, t := range []reflect.Type{req, resp} {
			if t.PkgPath() != svc.ImportPath {
				return nil, fmt.Errorf("%s: type %s is in %q, expected %q", m.Name, t.Name(), t.PkgPath(), svc.ImportPath)
			}
		}

		methods = append(methods, method{Name: m.Name, Req: req.Name(), Resp: resp.Name()})
	}

	if len(methods) == 0 {
		return nil, fmt.Errorf("no unary RPC methods found on %T", svc.Type)
	}

	return methods, nil
}

func isUnaryRPC(t reflect.Type) bool {
	return !t.IsVariadic() &&
		t.NumIn() == 2 && t.In(0) == contextType && t.In(1).Kind() == reflect.Pointer &&
		t.NumOut() == 2 && t.Out(0).Kind() == reflect.Pointer && t.Out(1) == errorType
}
