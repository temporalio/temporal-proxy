package router

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/temporalio/temporal-proxy/pkg/match"
)

func TestMuxSwitch(t *testing.T) {
	t.Parallel()

	mux := New(
		"default",
		"system",
		Rule{upstream: "prod", ns: match.MustCompile("prod-*")},
		Rule{upstream: "gold", ns: match.MustCompile("*"), meta: map[string]match.Matcher{"x-tier": match.MustCompile("gold")}},
		Rule{upstream: "combo", ns: match.MustCompile("eu-*"), meta: map[string]match.Matcher{"x-region": match.MustCompile("eu*")}},
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
		Rule{upstream: "first", ns: match.MustCompile("prod-*")},
		Rule{upstream: "second", ns: match.MustCompile("prod-*")},
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
		Rule{upstream: "prod", ns: match.MustCompile("prod-*")},
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
		Rule{upstream: "", ns: match.MustCompile("*")},
	)

	got, outcome := mux.Switch("other", nil)
	require.Equal(t, "", got, "a matching rule with an empty upstream still yields no upstream")
	require.Equal(t, OutcomeUnroutable, outcome)
}

func TestMuxSwitchZeroMatcherMatchesOnlyEmpty(t *testing.T) {
	t.Parallel()

	// A Rule whose namespace matcher was never compiled must not act as a
	// catch-all. The zero match.Matcher matches only the empty string.
	mux := New(
		"default",
		"",
		Rule{upstream: "unset"},
	)

	got, outcome := mux.Switch("anything", nil)
	require.Equal(t, "default", got)
	require.Equal(t, OutcomeDefault, outcome)

	got, outcome = mux.Switch("", nil)
	require.Equal(t, "unset", got)
	require.Equal(t, OutcomeMatch, outcome)
}
