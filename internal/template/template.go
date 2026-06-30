package template

import (
	"fmt"
	"strings"
	texttemplate "text/template"
)

var probeMeta = map[string]string{
	"dc":        "probe",
	"x-cluster": "probe",
}

type (
	// RoutingContext is available when evaluating routing rules, before an
	// upstream has been selected. It deliberately omits the remote namespace:
	// namespace translation is defined per upstream, so the remote name is not
	// known until routing has chosen one.
	RoutingContext struct {
		LocalNamespace string
		Metadata       map[string]string
	}

	// UpstreamContext is available when rendering an upstream's hostPort and
	// serverName, after routing has selected the upstream and the remote
	// namespace is known.
	UpstreamContext struct {
		LocalNamespace  string
		RemoteNamespace string
		Metadata        map[string]string
	}

	// Template is a compiled template bound to the context type T it renders
	// against. Construct one with [ParseRouting] or [ParseUpstream].
	Template[T any] struct {
		tmpl *texttemplate.Template
		raw  string
	}
)

// ParseRouting compiles s into a [Template] rendered against a [RoutingContext].
// A reference to a field absent from that context (such as RemoteNamespace)
// fails here rather than at request time.
func ParseRouting(s string) (*Template[RoutingContext], error) {
	return parse(s, RoutingContext{LocalNamespace: "probe", Metadata: probeMeta})
}

// ParseUpstream compiles s into a [Template] rendered against an
// [UpstreamContext].
func ParseUpstream(s string) (*Template[UpstreamContext], error) {
	return parse(s, UpstreamContext{LocalNamespace: "probe", RemoteNamespace: "probe", Metadata: probeMeta})
}

// Must returns t or panics if err is non-nil. It wraps a Parse call for
// package-level variables and tests where an invalid template is a programming
// error.
func Must[T any](t *Template[T], err error) *Template[T] {
	if err != nil {
		panic(err)
	}

	return t
}

// Render evaluates the template against ctx. A reference to an absent metadata
// key renders as the empty string.
func (t *Template[T]) Render(ctx T) (string, error) {
	var sb strings.Builder
	if err := t.tmpl.Execute(&sb, ctx); err != nil {
		return "", fmt.Errorf("template: render %q: %w", t.raw, err)
	}

	return sb.String(), nil
}

// String returns the raw template source.
func (t *Template[T]) String() string {
	return t.raw
}

// parse compiles s and validates it by rendering against probe, surfacing
// references to unknown fields at parse time. Absent metadata keys render empty
// under missingkey=zero and so do not trip the check.
func parse[T any](s string, probe T) (*Template[T], error) {
	tmpl, err := texttemplate.New("template").Option("missingkey=zero").Parse(s)
	if err != nil {
		return nil, fmt.Errorf("template: parse %q: %w", s, err)
	}

	t := &Template[T]{tmpl: tmpl, raw: s}
	if _, err := t.Render(probe); err != nil {
		return nil, err
	}

	return t, nil
}
