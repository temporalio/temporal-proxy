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

func TestConfigPrepareRoutingCompileErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		match      config.RoutingMatch
		wantTuples [][2]string
	}{
		{
			name:       "bad namespace glob",
			match:      config.RoutingMatch{Namespace: "a*b"},
			wantTuples: [][2]string{{"routing.rules[0]", "namespace"}},
		},
		{
			name:       "bad metadata glob",
			match:      config.RoutingMatch{Metadata: map[string]string{"dc": "a*b"}},
			wantTuples: [][2]string{{"routing.rules[0]", "metadata[dc]"}},
		},
		{
			name:       "metadata keys collide when lowercased",
			match:      config.RoutingMatch{Metadata: map[string]string{"DC": "a", "dc": "b"}},
			wantTuples: [][2]string{{"routing.rules[0]", "metadata"}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cfg := &config.Config{
				Listen:    config.ListenConfig{HostPort: ":8080"},
				Upstreams: []config.Upstream{{Name: "primary", Listen: config.ListenConfig{HostPort: "127.0.0.1:7233"}}},
				Routing:   config.Routing{Rules: []config.RoutingRule{{Upstream: "primary", Match: tt.match}}},
			}

			assertTuples(t, cfg.Prepare(), tt.wantTuples)
		})
	}
}

func TestRoutingRuleMatchers(t *testing.T) {
	t.Parallel()

	rule := config.RoutingRule{Upstream: "primary", Match: config.RoutingMatch{Metadata: map[string]string{"DC": "us-*"}}}
	require.Panics(t, func() { rule.Matchers() })

	cfg := &config.Config{
		Listen:    config.ListenConfig{HostPort: ":8080"},
		Upstreams: []config.Upstream{{Name: "primary", Listen: config.ListenConfig{HostPort: "127.0.0.1:7233"}}},
		Routing:   config.Routing{Rules: []config.RoutingRule{rule}},
	}
	require.NoError(t, cfg.Prepare())

	ns, meta := cfg.Routing.Rules[0].Matchers()
	require.True(t, ns.Match("anything"), "an empty namespace pattern matches every namespace")
	require.True(t, meta["dc"].Match("us-east"), "metadata keys are lowercased")
}
