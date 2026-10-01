package rpc_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/metadata"

	"github.com/temporalio/temporal-proxy/internal/rpc"
)

func TestIncomingIsAPrivateCopy(t *testing.T) {
	t.Parallel()

	// Guard: Incoming hands back gRPC's accessor result without copying it again,
	// which is only safe while that accessor builds a fresh map. gRPC does not
	// document that it does, so this fails if an upgrade starts sharing the map.
	original := metadata.Pairs("authorization", "Bearer k3y", "x-keep", "kept")
	ctx := metadata.NewIncomingContext(t.Context(), original)

	md := rpc.Incoming(ctx)
	md.Delete("authorization")
	md.Set("x-added", "added")
	md["x-keep"][0] = "changed"

	require.Equal(t, []string{"Bearer k3y"}, original.Get("authorization"), "expected the context's metadata to be untouched")
	require.Empty(t, original.Get("x-added"))
	require.Equal(t, []string{"kept"}, original.Get("x-keep"), "expected values to be copied, not shared")
	require.Equal(t, []string{"kept"}, rpc.Incoming(ctx).Get("x-keep"))
}

func TestIncomingIsEmptyWithoutMetadata(t *testing.T) {
	t.Parallel()

	md := rpc.Incoming(t.Context())
	require.NotNil(t, md, "expected empty metadata rather than nil, so callers can add keys")
	require.Empty(t, md)
}

func TestOutgoingIsAPrivateCopy(t *testing.T) {
	t.Parallel()

	// Guard: the same contract as Incoming, for the outgoing accessor. Pairs added
	// with AppendToOutgoingContext are held apart from the base map, so both are
	// covered.
	original := metadata.Pairs("x-keep", "kept")
	ctx := metadata.AppendToOutgoingContext(metadata.NewOutgoingContext(t.Context(), original), "x-appended", "appended")

	md := rpc.Outgoing(ctx)
	md.Set("x-added", "added")
	md["x-keep"][0] = "changed"
	md["x-appended"][0] = "changed"

	require.Empty(t, original.Get("x-added"), "expected the context's metadata to be untouched")
	require.Equal(t, []string{"kept"}, original.Get("x-keep"), "expected values to be copied, not shared")

	again := rpc.Outgoing(ctx)
	require.Equal(t, []string{"kept"}, again.Get("x-keep"))
	require.Equal(t, []string{"appended"}, again.Get("x-appended"))
	require.Empty(t, again.Get("x-added"))
}

func TestOutgoingIsEmptyWithoutMetadata(t *testing.T) {
	t.Parallel()

	md := rpc.Outgoing(t.Context())
	require.NotNil(t, md, "expected empty metadata rather than nil, so callers can add keys")
	require.Empty(t, md)
}

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
