package services_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/temporalio/temporal-proxy/internal/services"
)

func TestResolveKnownServices(t *testing.T) {
	t.Parallel()

	for _, name := range services.Expand(services.Known()) {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			sd, err := services.Resolve(name)
			require.NoError(t, err)
			require.Equal(t, name, string(sd.FullName()))
			require.Positive(t, sd.Methods().Len(), "a forwardable service must have methods")
		})
	}
}

func TestResolveUnknownService(t *testing.T) {
	t.Parallel()

	_, err := services.Resolve("temporal.api.nope.v1.NopeService")
	require.Error(t, err)
	require.Contains(t, err.Error(), "temporal.api.nope.v1.NopeService")
}

func TestResolveRejectsNonService(t *testing.T) {
	t.Parallel()

	// A message type resolves in the registry but is not a service.
	_, err := services.Resolve("temporal.api.common.v1.Payload")
	require.Error(t, err)
	require.Contains(t, err.Error(), "not a service")
}

func TestExpandAddsReflectionAlias(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		in   []string
		want []string
	}{
		{
			name: "reflection gains its v1alpha alias",
			in:   []string{services.Reflection},
			want: []string{services.Reflection, services.ReflectionV1Alpha},
		},
		{
			name: "services without aliases pass through",
			in:   []string{services.WorkflowService, services.OperatorService},
			want: []string{services.WorkflowService, services.OperatorService},
		},
		{
			name: "an explicit alias is not duplicated",
			in:   []string{services.Reflection, services.ReflectionV1Alpha},
			want: []string{services.Reflection, services.ReflectionV1Alpha},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			require.ElementsMatch(t, tt.want, services.Expand(tt.in))
		})
	}
}

func TestDefaultIsForwardableSubsetOfKnown(t *testing.T) {
	t.Parallel()

	require.Equal(t, []string{services.WorkflowService, services.OperatorService}, services.Default())
	require.Subset(t, services.Known(), services.Default())
}

func TestAllIsKnownExpanded(t *testing.T) {
	t.Parallel()

	require.ElementsMatch(t, services.Expand(services.Known()), services.All())
	require.Contains(t, services.All(), services.ReflectionV1Alpha,
		"All must carry the alias Known omits, since it is what admission checks against")
}
