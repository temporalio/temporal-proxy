package services_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/temporalio/temporal-proxy/internal/services"
)

func TestAllowlistAllows(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		allowed []string
		want    map[string]bool
	}{
		{
			name:    "the default set admits the two default services and nothing else",
			allowed: services.Default(),
			want: map[string]bool{
				services.WorkflowService:   true,
				services.OperatorService:   true,
				services.Reflection:        false,
				services.ReflectionV1Alpha: false,
			},
		},
		{
			name:    "allowing reflection also allows its alias",
			allowed: []string{services.Reflection},
			want: map[string]bool{
				services.Reflection:        true,
				services.ReflectionV1Alpha: true,
				services.WorkflowService:   false,
			},
		},
		{
			// The alias relation is one-way: naming the superseded spelling does
			// not admit the current one.
			name:    "allowing only the alias does not allow the current spelling",
			allowed: []string{services.ReflectionV1Alpha},
			want: map[string]bool{
				services.ReflectionV1Alpha: true,
				services.Reflection:        false,
			},
		},
		{
			name:    "the full known set admits every forwardable name",
			allowed: services.Known(),
			want: map[string]bool{
				services.WorkflowService:   true,
				services.OperatorService:   true,
				services.Reflection:        true,
				services.ReflectionV1Alpha: true,
			},
		},
		{
			name:    "a nil list admits nothing",
			allowed: nil,
			want: map[string]bool{
				services.WorkflowService: false,
				"":                       false,
			},
		},
		{
			name:    "an empty list admits nothing",
			allowed: []string{},
			want: map[string]bool{
				services.WorkflowService: false,
			},
		},
		{
			// Names are matched literally, so a caller holding a full method
			// must strip it first.
			name:    "a full method does not match the service it names",
			allowed: []string{services.WorkflowService},
			want: map[string]bool{
				services.WorkflowService:                                   true,
				"/" + services.WorkflowService + "/StartWorkflowExecution": false,
			},
		},
		{
			// The allowlist trusts its input: rejecting unforwardable names is
			// configuration validation's job.
			name:    "an unregistered name is admitted verbatim",
			allowed: []string{"not.A.Thing"},
			want: map[string]bool{
				"not.A.Thing":            true,
				services.WorkflowService: false,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			allowlist := services.NewAllowlist(tt.allowed)
			for name, want := range tt.want {
				require.Equal(t, want, allowlist.Allows(name), "Allows(%q)", name)
			}
		})
	}
}

func TestNewAllowlistCollapsesDuplicates(t *testing.T) {
	t.Parallel()

	// Validation rejects a duplicated allowlist, but one is also constructible
	// in code, so collapsing duplicates must not change what it admits.
	allowlist := services.NewAllowlist([]string{services.WorkflowService, services.WorkflowService})

	require.Len(t, allowlist, 1)
	require.True(t, allowlist.Allows(services.WorkflowService))
	require.False(t, allowlist.Allows(services.OperatorService))
}
