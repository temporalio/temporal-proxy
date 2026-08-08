package rpc

import (
	"context"

	"google.golang.org/grpc/metadata"
)

// WithOutgoing returns ctx with fn applied to its outgoing gRPC metadata. fn
// receives a copy, never the metadata already on ctx: that copy is the whole
// point, because outgoing metadata is shared with whatever else holds the context
// and mutating it in place corrupts calls in flight. fn is handed empty metadata
// when ctx carries none, so a caller that only adds keys needs no special case.
func WithOutgoing(ctx context.Context, fn func(metadata.MD)) context.Context {
	md, ok := metadata.FromOutgoingContext(ctx)
	if !ok {
		md = metadata.MD{}
	} else {
		md = md.Copy()
	}

	fn(md)
	return metadata.NewOutgoingContext(ctx, md)
}
