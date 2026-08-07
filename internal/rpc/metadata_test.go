package rpc_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/metadata"

	"github.com/temporalio/temporal-proxy/internal/rpc"
)

func TestWithOutgoingLeavesTheCallersMetadataAlone(t *testing.T) {
	t.Parallel()

	// Guard: the metadata already on the context is shared with whatever else holds
	// it, so a caller that mutates in place corrupts calls in flight.
	original := metadata.Pairs("authorization", "Bearer k3y", "x-keep", "kept")
	ctx := metadata.NewOutgoingContext(t.Context(), original)

	out := rpc.WithOutgoing(ctx, func(md metadata.MD) {
		md.Delete("authorization")
		md.Set("x-added", "added")
	})

	require.Equal(t, []string{"Bearer k3y"}, original.Get("authorization"), "expected the caller's metadata to be untouched")
	require.Empty(t, original.Get("x-added"))

	got, ok := metadata.FromOutgoingContext(out)
	require.True(t, ok)
	require.Empty(t, got.Get("authorization"))
	require.Equal(t, []string{"added"}, got.Get("x-added"))
	require.Equal(t, []string{"kept"}, got.Get("x-keep"))
}

func TestWithOutgoingStartsFromEmptyMetadata(t *testing.T) {
	t.Parallel()

	// A context with no outgoing metadata is the common case on an inbound call, so
	// fn is handed empty metadata rather than nil and a caller that only adds keys
	// needs no special case.
	out := rpc.WithOutgoing(t.Context(), func(md metadata.MD) {
		require.Empty(t, md)
		md.Set("x-added", "added")
	})

	got, ok := metadata.FromOutgoingContext(out)
	require.True(t, ok)
	require.Equal(t, []string{"added"}, got.Get("x-added"))
}
