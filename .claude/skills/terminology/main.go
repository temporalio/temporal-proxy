// Command terminology checks the repository against the project's canonical
// vocabulary. It exits non-zero when it finds a violation, so it can run as a
// lint step.
package main

import (
	_ "embed"
	"fmt"
	"os"
	"os/exec"
	"slices"
	"strings"

	"github.com/temporalio/temporal-proxy/skills/terminology/terms"
)

// rules travels with the command so it reads the same rule set wherever it runs.
//
//go:embed terms.yaml
var rules string

func main() {
	root := "../../../"
	if len(os.Args) > 1 {
		root = os.Args[1]
	}

	if err := run(root); err != nil {
		fmt.Fprintln(os.Stderr, "terminology:", err)
		os.Exit(2)
	}
}

// gitFiles lists the files git knows about: tracked, plus untracked ones it is
// not ignoring. Asking git rather than reading .gitignore keeps one source of
// truth and picks up .git/info/exclude and nested ignore files for free.
func gitFiles(root string) ([]string, error) {
	cmd := exec.Command("git", "-C", root, "ls-files", "--cached", "--others", "--exclude-standard", "-z")

	out, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("git ls-files in %s: %w", root, err)
	}

	return slices.DeleteFunc(strings.Split(string(out), "\x00"), func(p string) bool {
		return p == ""
	}), nil
}

// run reports every violation under root, and errors when there is at least one.
func run(root string) error {
	cfg, err := terms.Load(strings.NewReader(rules))
	if err != nil {
		return err
	}

	paths, err := gitFiles(root)
	if err != nil {
		return err
	}

	findings, err := terms.Run(cfg, os.DirFS(root), paths)
	if err != nil {
		return err
	}

	for _, f := range findings {
		fmt.Printf("%s:%d: %s\n", f.Path, f.Line, f.Message)
	}

	if len(findings) > 0 {
		return fmt.Errorf("%d terminology violation(s)", len(findings))
	}

	return nil
}
