package main

import (
	"bytes"
	"context"
	"embed"
	"fmt"
	"go/format"
	"io"
	"log"
	"os"
	"strings"
	"text/template"

	"github.com/urfave/cli/v3"
)

var (
	//go:embed templates/*.go.tmpl
	templates embed.FS

	renderer = template.Must(template.New("").Funcs(template.FuncMap{
		"lower": strings.ToLower,
	}).ParseFS(templates, "templates/*.go.tmpl"))
)

func main() {
	app := &cli.Command{
		Name:  "codegen",
		Usage: "A custom tool for generating code for the proxy",
		Commands: []*cli.Command{
			proxyCommand(),
			visitorsCommand(),
		},
	}

	if err := app.Run(context.Background(), os.Args); err != nil {
		log.Fatalf("codegen error: %v", err)
	}
}

func render(w io.Writer, tmpl string, data any) error {
	if err := renderer.ExecuteTemplate(w, tmpl+".go.tmpl", data); err != nil {
		return fmt.Errorf("failed to exec template: %s, %w", tmpl, err)
	}

	return nil
}

func writeFile(tmpl, path string, data any) error {
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

	_, err = fmt.Printf("codegen: wrote %s\n", path)
	return err
}
