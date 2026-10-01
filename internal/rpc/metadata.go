package rpc

import (
	"context"

	"google.golang.org/grpc/metadata"
)

// Incoming returns ctx's incoming gRPC metadata as a map the caller owns, so it
// can be modified or attached to an outgoing context without copying it first.
// gRPC's accessor already returns a fresh copy but does not document it, so the
// assumption lives here, guarded by TestIncomingIsAPrivateCopy. It is empty,
// never nil, when ctx carries none.
func Incoming(ctx context.Context) metadata.MD {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return metadata.MD{}
	}

	return md
}

// Outgoing is [Incoming] for ctx's outgoing metadata. Changes to the returned
// map reach ctx only once it is attached with [metadata.NewOutgoingContext].
func Outgoing(ctx context.Context) metadata.MD {
	md, ok := metadata.FromOutgoingContext(ctx)
	if !ok {
		return metadata.MD{}
	}

	return md
}

// WithOutgoing returns ctx with fn applied to a copy of its outgoing gRPC
// metadata. The metadata on ctx is shared with whatever else holds the context,
// so it is never modified in place. fn gets empty metadata when ctx carries none.
func WithOutgoing(ctx context.Context, fn func(metadata.MD)) context.Context {
	md := Outgoing(ctx)
	fn(md)

	return metadata.NewOutgoingContext(ctx, md)
}
