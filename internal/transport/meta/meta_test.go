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
