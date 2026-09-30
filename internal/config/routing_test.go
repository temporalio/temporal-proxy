package config_test

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/temporalio/temporal-proxy/internal/config"
	"github.com/temporalio/temporal-proxy/pkg/validation"
)

func TestRouting_Validate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		routing    *config.Routing
		wantTuples [][2]string
	}{
		{
			name:    "empty routing yields no error",
			routing: &config.Routing{},
		},
		{
			name: "default and system are not checked for references here",
			routing: &config.Routing{
				DefaultUpstream: "unknown",
				SystemUpstream:  "also-unknown",
			},
		},
		{
			name: "valid rules yield no error",
			routing: &config.Routing{
				Rules: []config.RoutingRule{
					{Upstream: "primary", Match: config.RoutingMatch{Namespace: "payments"}},
					{Upstream: "system", Match: config.RoutingMatch{Metadata: map[string]string{"tier": "gold"}}},
				},
			},
		},
		{
			name: "invalid rule surfaces with its index",
			routing: &config.Routing{
				Rules: []config.RoutingRule{
					{Match: config.RoutingMatch{Namespace: "payments"}},
				},
			},
			wantTuples: [][2]string{{"rules[0]", "upstream"}},
		},
		{
			name: "invalid rules keep their own indices",
			routing: &config.Routing{
				Rules: []config.RoutingRule{
					{Upstream: "primary", Match: config.RoutingMatch{Namespace: "payments"}},
					{Match: config.RoutingMatch{}},
				},
			},
			wantTuples: [][2]string{{"rules[1]", "upstream"}, {"rules[1]", "namespace"}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assertTuples(t, tt.routing.Validate(), tt.wantTuples)
		})
	}
}

func TestRoutingRule_Validate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		rule       *config.RoutingRule
		wantTuples [][2]string
	}{
		{
			name: "valid upstream and match",
			rule: &config.RoutingRule{
				Upstream: "primary",
				Match:    config.RoutingMatch{Namespace: "payments"},
			},
		},
		{
			name: "missing upstream",
			rule: &config.RoutingRule{
				Match: config.RoutingMatch{Namespace: "payments"},
			},
			wantTuples: [][2]string{{"", "upstream"}},
		},
		{
			name:       "missing upstream and empty match aggregate",
			rule:       &config.RoutingRule{},
			wantTuples: [][2]string{{"", "upstream"}, {"", "namespace"}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assertTuples(t, tt.rule.Validate(), tt.wantTuples)
		})
	}
}

func TestRoutingMatch_Validate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		match      *config.RoutingMatch
		wantTuples [][2]string // (Subject, Field); empty slice means no error expected
	}{
		{
			name:  "namespace set, no metadata",
			match: &config.RoutingMatch{Namespace: "payments"},
		},
		{
			name:  "metadata set, no namespace",
			match: &config.RoutingMatch{Metadata: map[string]string{"tier": "gold"}},
		},
		{
			name: "both namespace and metadata set",
			match: &config.RoutingMatch{
				Namespace: "payments",
				Metadata:  map[string]string{"tier": "gold"},
			},
		},
		{
			name:       "neither namespace nor metadata set",
			match:      &config.RoutingMatch{},
			wantTuples: [][2]string{{"", "namespace"}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assertTuples(t, tt.match.Validate(), tt.wantTuples)
		})
	}
}

// assertTuples asserts that err carries exactly the (Subject, Field) pairs in
// want. An empty want means err must be nil.
func assertTuples(t *testing.T, err error, want [][2]string) {
	t.Helper()

	if len(want) == 0 {
		require.NoError(t, err)
		return
	}

	var errs validation.Errors
	require.True(t, errors.As(err, &errs), "expected validation.Errors, got %T", err)

	got := make([][2]string, len(errs))
	for i, e := range errs {
		got[i] = [2]string{e.Subject, e.Field}
	}

	require.ElementsMatch(t, want, got)
}
