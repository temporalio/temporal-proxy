package rpc

import (
	"context"

	"google.golang.org/grpc/metadata"
)

// Incoming returns ctx's incoming gRPC metadata as a map the caller owns: modify
// it, or attach it to an outgoing context, without copying it first. gRPC's
// accessor already builds a fresh map, with fresh value slices, on every call, so
// copying again only doubles the cost. gRPC does not document that it copies,
// which is why the assumption lives here, behind a test that fails if an upgrade
// stops doing so, rather than at each call site. It is empty, never nil, when ctx
// carries none.
func Incoming(ctx context.Context) metadata.MD {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return metadata.MD{}
	}

	return md
}

// Outgoing returns ctx's outgoing gRPC metadata as a map the caller owns, on the
// same terms as [Incoming]. Attach it with [metadata.NewOutgoingContext] once
// modified; changing it alone changes nothing on ctx.
func Outgoing(ctx context.Context) metadata.MD {
	md, ok := metadata.FromOutgoingContext(ctx)
	if !ok {
		return metadata.MD{}
	}

	return md
}

// WithOutgoing returns ctx with fn applied to its outgoing gRPC metadata. fn
// receives a map of its own from [Outgoing], never the metadata already on ctx,
// which is shared with whatever else holds the context, so mutating it in place
// would corrupt calls in flight. fn is handed empty metadata when ctx carries
// none, so a caller that only adds keys needs no special case.
func WithOutgoing(ctx context.Context, fn func(metadata.MD)) context.Context {
	md := Outgoing(ctx)
	fn(md)

	return metadata.NewOutgoingContext(ctx, md)
}
