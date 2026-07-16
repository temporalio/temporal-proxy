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
		name    string
		ns      string
		md      map[string][]string
		want    string
		outcome Outcome
	}{
		{name: "namespace prefix rule", ns: "prod-1", want: "prod", outcome: OutcomeMatch},
		{name: "metadata-only rule", ns: "anything", md: map[string][]string{"x-tier": {"gold"}}, want: "gold", outcome: OutcomeMatch},
		{name: "metadata any of many values", ns: "anything", md: map[string][]string{"x-tier": {"bronze", "gold"}}, want: "gold", outcome: OutcomeMatch},
		{name: "combined namespace and metadata", ns: "eu-1", md: map[string][]string{"x-region": {"eu-west"}}, want: "combo", outcome: OutcomeMatch},
		{name: "combined rule fails on metadata", ns: "eu-1", md: map[string][]string{"x-region": {"us-east"}}, want: "default", outcome: OutcomeDefault},
		{name: "metadata-only rule matches empty namespace", ns: "", md: map[string][]string{"x-tier": {"gold"}}, want: "gold", outcome: OutcomeMatch},
		{name: "constrained metadata key absent", ns: "other", md: map[string][]string{"unrelated": {"gold"}}, want: "default", outcome: OutcomeDefault},
		{name: "no namespace falls to system", ns: "", want: "system", outcome: OutcomeSystem},
		{name: "namespaced no match falls to default", ns: "other", want: "default", outcome: OutcomeDefault},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, outcome := mux.Switch(tt.ns, tt.md)
			require.Equal(t, tt.want, got)
			require.Equal(t, tt.outcome, outcome)
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

	got, outcome := mux.Switch("prod-1", nil)
	require.Equal(t, "first", got)
	require.Equal(t, OutcomeMatch, outcome)
}

func TestMuxSwitchNoDefault(t *testing.T) {
	t.Parallel()

	mux := New(
		"",
		"",
		Rule{upstream: "prod", ns: matchPrefix("prod-")},
	)

	got, outcome := mux.Switch("other", nil)
	require.Equal(t, "", got, "no rule and no default yields empty")
	require.Equal(t, OutcomeUnroutable, outcome)

	got, outcome = mux.Switch("", nil)
	require.Equal(t, "", got, "no namespace, no system, no default yields empty")
	require.Equal(t, OutcomeUnroutable, outcome)
}

func TestMuxSwitchEmptyRuleUpstreamIsUnroutable(t *testing.T) {
	t.Parallel()

	mux := New(
		"default",
		"",
		Rule{upstream: "", ns: matchAll()},
	)

	got, outcome := mux.Switch("other", nil)
	require.Equal(t, "", got, "a matching rule with an empty upstream still yields no upstream")
	require.Equal(t, OutcomeUnroutable, outcome)
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
