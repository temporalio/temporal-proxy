// Package meta defines the internal gRPC metadata contract carried between the
// router and the per-upstream proxy: the router stamps the request namespace it
// already extracted so the proxy can resolve a templated upstream address
// without parsing the payload again. It depends on no other internal packages.
package meta

import (
	"context"

	"google.golang.org/grpc/metadata"
)

// NamespaceHeader is the outgoing metadata key that carries the local (pre-
// translation) namespace from the router to the upstream proxy.
const NamespaceHeader = "x-temporal-proxy-namespace"

// WithNamespace returns ctx with namespace set on its outgoing gRPC metadata,
// replacing any value already present for NamespaceHeader (so a client cannot
// influence routing by sending the header itself).
func WithNamespace(ctx context.Context, namespace string) context.Context {
	md, ok := metadata.FromOutgoingContext(ctx)
	if !ok {
		md = metadata.MD{}
	} else {
		md = md.Copy()
	}

	md.Set(NamespaceHeader, namespace)
	return metadata.NewOutgoingContext(ctx, md)
}

// NamespaceFrom returns the namespace carried on ctx's outgoing metadata, or ""
// when absent. When multiple values are present the last (most recently added)
// wins.
func NamespaceFrom(ctx context.Context) string {
	md, ok := metadata.FromOutgoingContext(ctx)
	if !ok {
		return ""
	}

	vals := md.Get(NamespaceHeader)
	if len(vals) == 0 {
		return ""
	}

	return vals[len(vals)-1]
}
