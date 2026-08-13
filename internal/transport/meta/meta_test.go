package meta_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/metadata"

	"github.com/temporalio/temporal-proxy/internal/transport/meta"
)

func TestWithNamespaceRoundTrips(t *testing.T) {
	t.Parallel()

	ctx := meta.WithNamespace(t.Context(), "orders")
	require.Equal(t, "orders", meta.NamespaceFrom(ctx))
}

func TestWithNamespaceOverwritesExisting(t *testing.T) {
	t.Parallel()

	// A spoofed value already present in outgoing metadata is replaced.
	ctx := metadata.NewOutgoingContext(t.Context(), metadata.Pairs(meta.NamespaceHeader, "spoofed"))
	ctx = meta.WithNamespace(ctx, "orders")

	md, _ := metadata.FromOutgoingContext(ctx)
	require.Equal(t, []string{"orders"}, md.Get(meta.NamespaceHeader))
}

func TestNamespaceFromReturnsLastValue(t *testing.T) {
	t.Parallel()

	ctx := metadata.NewOutgoingContext(t.Context(), metadata.Pairs(
		meta.NamespaceHeader, "old",
		meta.NamespaceHeader, "new",
	))
	require.Equal(t, "new", meta.NamespaceFrom(ctx))
}

func TestNamespaceFromAbsentIsEmpty(t *testing.T) {
	t.Parallel()

	require.Equal(t, "", meta.NamespaceFrom(t.Context()))
}

func TestWithTargetRoundTrips(t *testing.T) {
	t.Parallel()

	target := meta.Target{FullName: "/temporal.api.workflowservice.v1.WorkflowService/StartWorkflowExecution", Namespace: "orders"}

	ctx := meta.WithTarget(t.Context(), target)
	require.Equal(t, target, meta.TargetFrom(ctx))
}

func TestTargetFromAbsentIsZero(t *testing.T) {
	t.Parallel()

	require.Equal(t, meta.Target{}, meta.TargetFrom(t.Context()))
}

func TestTargetFromIgnoresMetadata(t *testing.T) {
	t.Parallel()

	// A caller cannot forge a Target by sending metadata: it is carried as a
	// context value the gateway sets, and authentication decides on it.
	ctx := metadata.NewIncomingContext(t.Context(), metadata.Pairs(meta.NamespaceHeader, "spoofed"))
	require.Equal(t, meta.Target{}, meta.TargetFrom(ctx))
}
