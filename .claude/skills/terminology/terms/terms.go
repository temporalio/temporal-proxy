// Package terms checks the repository against the project's canonical
// vocabulary: phrases that have been rejected in favour of another term, and
// Temporal's core nouns, which are proper nouns in prose.
package terms

import (
	"fmt"
	"io"
	"io/fs"
	"path"
	"regexp"
	"sort"
	"strings"

	"github.com/goccy/go-yaml"
)

var (
	// protected masks the markdown spans that are not prose: inline code, link
	// destinations, and autolinks. Lowercase inside them is correct, since they
	// carry config keys, CLI commands, and URLs.
	protected = regexp.MustCompile("`[^`\n]*`" + `|\]\([^)]*\)|<[^>\s]+>`)

	// refDef matches a link reference definition, which is all target and no prose.
	refDef = regexp.MustCompile(`^\[[^\]]+\]:`)
)

type (
	// Banned is one rejected phrase and the term to use instead. Except lists
	// path fragments where the phrase is legitimate.
	Banned struct {
		Phrase string   `yaml:"phrase"`
		Use    string   `yaml:"use"`
		Except []string `yaml:"except"`
	}

	// Core is a Temporal noun that is a proper noun in prose. Except lists path
	// fragments where the lowercase spelling means something else.
	Core struct {
		Word   string   `yaml:"word"`
		Except []string `yaml:"except"`
	}

	// Config is the rule set the checker enforces.
	Config struct {
		IgnorePaths []string `yaml:"ignorePaths"`
		Banned      []Banned `yaml:"banned"`
		Capitalize  []Core   `yaml:"capitalize"`
	}

	// Finding is one violation, located for an editor to jump to.
	Finding struct {
		Path    string
		Line    int
		Message string
	}
)

// Check reports the terminology violations in content. Casing rules apply to
// markdown only: Go comments in this repo use lowercase "namespace" throughout
// and are consistent as they are.
func Check(cfg Config, path, content string) []Finding {
	if ignored(cfg.IgnorePaths, path) {
		return nil
	}

	var (
		out      []Finding
		inFence  bool
		markdown = strings.HasSuffix(path, ".md")
		casing   = casingRules(cfg.Capitalize, path)
	)

	for i, line := range strings.Split(content, "\n") {
		if markdown && strings.HasPrefix(strings.TrimSpace(line), "```") {
			inFence = !inFence

			continue
		}

		if inFence {
			continue
		}

		out = append(out, bannedIn(cfg, path, line, i+1)...)

		if markdown {
			out = append(out, miscasedIn(casing, path, mask(line), i+1)...)
		}
	}

	return out
}

// Load reads a rule set. Unknown keys are an error: a typo in the rule set
// would otherwise silently disable a rule rather than report one.
func Load(r io.Reader) (Config, error) {
	var cfg Config

	if err := yaml.NewDecoder(r, yaml.DisallowUnknownField()).Decode(&cfg); err != nil {
		return Config{}, fmt.Errorf("decode terms: %w", err)
	}

	return cfg, nil
}

// Run checks paths against cfg, reading each one from fsys, and reports the
// violations sorted by location so output is stable across runs. The caller
// chooses the paths: which files are in scope is git's business, not ours.
func Run(cfg Config, fsys fs.FS, paths []string) ([]Finding, error) {
	var out []Finding

	for _, p := range paths {
		if !checkable(p) || ignored(cfg.IgnorePaths, p) {
			continue
		}

		b, err := fs.ReadFile(fsys, p)
		if err != nil {
			return nil, fmt.Errorf("read %s: %w", p, err)
		}

		out = append(out, Check(cfg, p, string(b))...)
	}

	sort.Slice(out, func(i, j int) bool {
		if out[i].Path != out[j].Path {
			return out[i].Path < out[j].Path
		}

		if out[i].Line != out[j].Line {
			return out[i].Line < out[j].Line
		}

		return out[i].Message < out[j].Message
	})

	return out, nil
}

// bannedIn reports the rejected phrases appearing on one line.
func bannedIn(cfg Config, path, line string, num int) []Finding {
	var (
		out   []Finding
		lower = strings.ToLower(line)
	)

	for _, b := range cfg.Banned {
		if exempt(b.Except, path) {
			continue
		}

		if strings.Contains(lower, strings.ToLower(b.Phrase)) {
			out = append(out, Finding{
				Path:    path,
				Line:    num,
				Message: fmt.Sprintf("%q: use %q instead", b.Phrase, b.Use),
			})
		}
	}

	return out
}

// checkable reports whether a file's contents are prose the checker understands.
func checkable(p string) bool {
	switch path.Ext(p) {
	case ".go", ".md", ".yaml", ".yml":
		return true
	default:
		return false
	}
}

// casingRules compiles one word-boundary matcher per core noun that applies to
// path. The match is case sensitive so an already-capitalised term is left
// alone, and the optional plural keeps "namespaced" from matching.
func casingRules(words []Core, path string) map[string]*regexp.Regexp {
	rules := make(map[string]*regexp.Regexp, len(words))

	for _, w := range words {
		if exempt(w.Except, path) {
			continue
		}

		rules[w.Word] = regexp.MustCompile(`\b` + regexp.QuoteMeta(w.Word) + `s?\b`)
	}

	return rules
}

// exempt reports whether path matches one of a phrase's allowed locations. Each
// entry is a plain substring, so a directory ("dataplanetest/") and a file
// suffix ("_test.go") are both expressible without glob syntax.
func exempt(patterns []string, path string) bool {
	for _, p := range patterns {
		if strings.Contains(path, p) {
			return true
		}
	}

	return false
}

// ignored reports whether path sits under one of the ignored prefixes.
func ignored(prefixes []string, path string) bool {
	for _, p := range prefixes {
		if strings.HasPrefix(path, p) {
			return true
		}
	}

	return false
}

// mask blanks the spans of a markdown line that are not prose, preserving
// length so the remaining text keeps its offsets.
func mask(line string) string {
	if refDef.MatchString(strings.TrimSpace(line)) {
		return ""
	}

	return protected.ReplaceAllStringFunc(line, func(m string) string {
		return strings.Repeat(" ", len(m))
	})
}

// miscasedIn reports the core nouns written lowercase in one line of prose.
func miscasedIn(rules map[string]*regexp.Regexp, path, prose string, num int) []Finding {
	var out []Finding

	for word, re := range rules {
		for range re.FindAllString(prose, -1) {
			out = append(out, Finding{
				Path:    path,
				Line:    num,
				Message: fmt.Sprintf("%q in prose: capitalise as %q", word, title(word)),
			})
		}
	}

	return out
}

// title upper-cases the first letter of a lowercase term.
func title(w string) string {
	if w == "" {
		return w
	}

	return strings.ToUpper(w[:1]) + w[1:]
}
