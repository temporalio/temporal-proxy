package services_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/temporalio/temporal-proxy/internal/services"
)

func TestResolveKnownServices(t *testing.T) {
	t.Parallel()

	for _, name := range services.All() {
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

func TestDefaultIsForwardableSubsetOfKnown(t *testing.T) {
	t.Parallel()

	require.Equal(t,
		[]string{services.WorkflowService, services.OperatorService, services.CloudService},
		services.Default(),
	)
	require.Subset(t, services.Known(), services.Default())
}

func TestAllCarriesAliasesWithoutDuplicates(t *testing.T) {
	t.Parallel()

	all := services.All()
	require.Subset(t, all, services.Known())
	require.Contains(t, all, services.ReflectionV1Alpha,
		"All must carry the alias Known omits, since it is what admission checks against")

	seen := make(map[string]struct{}, len(all))
	for _, name := range all {
		_, dup := seen[name]
		require.False(t, dup, "All must not repeat %q", name)
		seen[name] = struct{}{}
	}
}
