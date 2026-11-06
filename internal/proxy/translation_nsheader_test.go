package proxy

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/metadata"

	"go.temporal.io/proxy/internal/transport/meta"
)

func TestRewriteNamespaceHeader(t *testing.T) {
	t.Parallel()

	remote := func(s string) string { return s + ".acct" }

	t.Run("rewrites the outgoing temporal-namespace header", func(t *testing.T) {
		t.Parallel()
		ctx := metadata.NewOutgoingContext(t.Context(), metadata.Pairs(meta.TemporalNamespaceHeader, "ns1"))

		md, ok := metadata.FromOutgoingContext(rewriteNamespaceHeader(ctx, remote))
		require.True(t, ok)
		require.Equal(t, []string{"ns1.acct"}, md.Get(meta.TemporalNamespaceHeader))
	})

	t.Run("no-op when the header is absent", func(t *testing.T) {
		t.Parallel()
		ctx := metadata.NewOutgoingContext(t.Context(), metadata.Pairs("other", "x"))

		md, _ := metadata.FromOutgoingContext(rewriteNamespaceHeader(ctx, remote))
		require.Empty(t, md.Get(meta.TemporalNamespaceHeader))
	})

	t.Run("no-op when there is no outgoing metadata", func(t *testing.T) {
		t.Parallel()

		_, ok := metadata.FromOutgoingContext(rewriteNamespaceHeader(t.Context(), remote))
		require.False(t, ok)
	})
}
