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

func TestAllowlistServiceNames(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		allowed []string
		want    []string
	}{
		{
			name:    "the default set names every default service",
			allowed: services.Default(),
			want:    []string{services.CloudService, services.OperatorService, services.WorkflowService},
		},
		{
			name:    "reflection names both spellings, since both are admitted",
			allowed: []string{services.Reflection},
			want:    []string{services.Reflection, services.ReflectionV1Alpha},
		},
		{
			name:    "a repeated name is named once",
			allowed: []string{services.WorkflowService, services.WorkflowService},
			want:    []string{services.WorkflowService},
		},
		{
			name:    "an empty list names nothing",
			allowed: nil,
			want:    nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// Sorted output, so callers publishing these (health entries, log
			// lines) get a stable order rather than map iteration order.
			require.Equal(t, tt.want, services.NewAllowlist(tt.allowed).ServiceNames())
		})
	}
}

func TestNewAllowlistToleratesDuplicates(t *testing.T) {
	t.Parallel()

	// Validation rejects a duplicated allowlist, but one is also constructible
	// in code, so a repeated name must not change what is admitted.
	a := services.NewAllowlist([]string{services.WorkflowService, services.WorkflowService})

	require.True(t, a.Allows(services.WorkflowService))
	require.False(t, a.Allows(services.OperatorService))
}
