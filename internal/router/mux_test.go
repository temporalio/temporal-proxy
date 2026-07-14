package router

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

type matcherFunc func(string) bool

func TestMuxSwitch(t *testing.T) {
	t.Parallel()

	mux := New(
		"default",
		"system",
		Rule{upstream: "prod", ns: matchPrefix("prod-")},
		Rule{upstream: "gold", ns: matchAll(), meta: map[string]Matcher{"x-tier": matchExact("gold")}},
		Rule{upstream: "combo", ns: matchPrefix("eu-"), meta: map[string]Matcher{"x-region": matchPrefix("eu")}},
	)

	tests := []struct {
		name string
		ns   string
		md   map[string][]string
		want string
	}{
		{name: "namespace prefix rule", ns: "prod-1", want: "prod"},
		{name: "metadata-only rule", ns: "anything", md: map[string][]string{"x-tier": {"gold"}}, want: "gold"},
		{name: "metadata any of many values", ns: "anything", md: map[string][]string{"x-tier": {"bronze", "gold"}}, want: "gold"},
		{name: "combined namespace and metadata", ns: "eu-1", md: map[string][]string{"x-region": {"eu-west"}}, want: "combo"},
		{name: "combined rule fails on metadata", ns: "eu-1", md: map[string][]string{"x-region": {"us-east"}}, want: "default"},
		{name: "metadata-only rule matches empty namespace", ns: "", md: map[string][]string{"x-tier": {"gold"}}, want: "gold"},
		{name: "constrained metadata key absent", ns: "other", md: map[string][]string{"unrelated": {"gold"}}, want: "default"},
		{name: "no namespace falls to system", ns: "", want: "system"},
		{name: "namespaced no match falls to default", ns: "other", want: "default"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tt.want, mux.Switch(tt.ns, tt.md))
		})
	}
}

func TestMuxSwitchFirstMatchWins(t *testing.T) {
	t.Parallel()

	mux := New(
		"default",
		"",
		Rule{upstream: "first", ns: matchPrefix("prod-")},
		Rule{upstream: "second", ns: matchPrefix("prod-")},
	)

	require.Equal(t, "first", mux.Switch("prod-1", nil))
}

func TestMuxSwitchNoDefault(t *testing.T) {
	t.Parallel()

	mux := New(
		"",
		"",
		Rule{upstream: "prod", ns: matchPrefix("prod-")},
	)

	require.Equal(t, "", mux.Switch("other", nil), "no rule and no default yields empty")
	require.Equal(t, "", mux.Switch("", nil), "no namespace, no system, no default yields empty")
}

func (f matcherFunc) Match(s string) bool { return f(s) }

func matchExact(want string) Matcher {
	return matcherFunc(func(s string) bool { return s == want })
}

func matchPrefix(p string) Matcher {
	return matcherFunc(func(s string) bool { return strings.HasPrefix(s, p) })
}

func matchAll() Matcher {
	return matcherFunc(func(string) bool { return true })
}
