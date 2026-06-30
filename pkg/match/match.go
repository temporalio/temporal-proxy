package match

import (
	"fmt"
	"strings"
)

const (
	literal  kind = iota
	prefix   kind = iota
	suffix   kind = iota
	contains kind = iota
	all      kind = iota
)

type (
	// Matcher is a compiled glob pattern. The zero value matches only the empty
	// string. Construct one with [Compile].
	Matcher struct {
		kind kind
		text string
	}

	kind int
)

// Compile parses pattern into a [Matcher]. See the package docs for the
// supported forms.
func Compile(pattern string) (Matcher, error) {
	leading := strings.HasPrefix(pattern, "*")
	trailing := strings.HasSuffix(pattern, "*")

	var m Matcher
	switch {
	case len(pattern) > 0 && strings.Count(pattern, "*") == len(pattern):
		return Matcher{kind: all}, nil
	case leading && trailing:
		m = Matcher{kind: contains, text: pattern[1 : len(pattern)-1]}
	case leading:
		m = Matcher{kind: suffix, text: strings.TrimPrefix(pattern, "*")}
	case trailing:
		m = Matcher{kind: prefix, text: strings.TrimSuffix(pattern, "*")}
	default:
		m = Matcher{kind: literal, text: pattern}
	}

	// The literal portion must be free of wildcards; a "*" here means the
	// pattern had one in an unsupported interior position (e.g. "a*b").
	if strings.Contains(m.text, "*") {
		return Matcher{}, fmt.Errorf("match: %q has a '*' outside the leading or trailing position", pattern)
	}

	return m, nil
}

// MustCompile is like [Compile] but panics if pattern is invalid. It is meant
// for package-level variables and tests where an invalid pattern is a
// programming error.
func MustCompile(pattern string) Matcher {
	m, err := Compile(pattern)
	if err != nil {
		panic(err)
	}

	return m
}

// Match reports whether s satisfies the compiled pattern.
func (m Matcher) Match(s string) bool {
	switch m.kind {
	case literal:
		return s == m.text
	case prefix:
		return strings.HasPrefix(s, m.text)
	case suffix:
		return strings.HasSuffix(s, m.text)
	case contains:
		return strings.Contains(s, m.text)
	case all:
		return true
	default:
		return false
	}
}
