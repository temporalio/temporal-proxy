package match_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/temporalio/temporal-proxy/pkg/match"
)

func TestMatcher(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		pattern string
		matches []string
		misses  []string
	}{
		{
			name:    "literal",
			pattern: "foo",
			matches: []string{"foo"},
			misses:  []string{"foobar", "barfoo", ""},
		},
		{
			name:    "prefix",
			pattern: "foo*",
			matches: []string{"foo", "foobar"},
			misses:  []string{"barfoo", ""},
		},
		{
			name:    "suffix",
			pattern: "*foo",
			matches: []string{"foo", "barfoo"},
			misses:  []string{"foobar", ""},
		},
		{
			name:    "contains",
			pattern: "*-test-*",
			matches: []string{"prod-test-1", "-test-"},
			misses:  []string{"test", "prod-1", ""},
		},
		{
			name:    "bare star matches anything",
			pattern: "*",
			matches: []string{"anything", ""},
		},
		{
			name:    "all stars matches anything",
			pattern: "**",
			matches: []string{"anything", ""},
		},
		{
			name:    "empty pattern is literal empty",
			pattern: "",
			matches: []string{""},
			misses:  []string{"foo"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			m, err := match.Compile(tt.pattern)
			require.NoError(t, err)

			for _, s := range tt.matches {
				require.Truef(t, m.Match(s), "pattern %q should match %q", tt.pattern, s)
			}
			for _, s := range tt.misses {
				require.Falsef(t, m.Match(s), "pattern %q should not match %q", tt.pattern, s)
			}
		})
	}
}

func TestCompileEmbeddedStar(t *testing.T) {
	t.Parallel()

	for _, pattern := range []string{"a*b", "*a*b", "a*b*", "a*b*c"} {
		_, err := match.Compile(pattern)
		require.Errorf(t, err, "pattern %q should be rejected", pattern)
	}
}

func TestMustCompile(t *testing.T) {
	t.Parallel()

	require.NotPanics(t, func() { match.MustCompile("foo*") })
	require.Panics(t, func() { match.MustCompile("a*b") })
}
