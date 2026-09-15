// Package meta defines the internal contract for what the gateway learns about a
// request once and every later stage reads: the [Target] on the context, and the
// namespace stamped on outgoing metadata for the per-upstream proxy. It depends
// on no other internal packages.
package meta

import (
	"context"

	"google.golang.org/grpc/metadata"
)

const (
	// NamespaceHeader is the outgoing metadata key that carries the local (pre-
	// translation) namespace from the router to the upstream proxy.
	NamespaceHeader = "x-temporal-proxy-namespace"

	// VersionHeader is the outgoing metadata key that carries the proxy's own
	// build version to the upstream. Only a Temporal Cloud upstream is sent one.
	VersionHeader = "x-temporal-proxy-version"
)

type (
	// Target is what a request is addressing: the gRPC full method and the
	// Temporal namespace named in its first message. Namespace is "" when the
	// request names none, when its payload could not be read, or when the gateway
	// does not forward the method's service and so never read one.
	Target struct {
		FullName  string
		Namespace string
	}

	// targetKey keys a Target on a context. It is an unexported type so no other
	// package can collide with or forge the entry.
	targetKey struct{}
)

// WithTarget returns ctx carrying target. It travels as a context value rather
// than as metadata because a caller must not be able to forge it: the gateway
// derives a Target from the stream it accepted, and authentication decides on it.
func WithTarget(ctx context.Context, target Target) context.Context {
	return context.WithValue(ctx, targetKey{}, target)
}

// TargetFrom returns the Target carried on ctx, or the zero Target when absent.
// A zero Target is the honest answer for a request the gateway has not inspected,
// so callers weighing one must treat empty fields as "unknown" rather than as
// values to match on.
func TargetFrom(ctx context.Context) Target {
	target, _ := ctx.Value(targetKey{}).(Target)
	return target
}

// WithNamespace returns ctx with namespace set on its outgoing gRPC metadata,
// replacing any value already present for NamespaceHeader (so a client cannot
// influence routing by sending the header itself).
func WithNamespace(ctx context.Context, namespace string) context.Context {
	return withHeader(ctx, NamespaceHeader, namespace)
}

// WithVersion returns ctx with version set on its outgoing gRPC metadata for
// VersionHeader. Like WithNamespace it replaces any value already there: the
// forwarder relays the caller's own metadata onward, so a client sending the
// header itself would otherwise reach the upstream alongside the real version.
func WithVersion(ctx context.Context, version string) context.Context {
	return withHeader(ctx, VersionHeader, version)
}

// withHeader returns ctx with key set to value on its outgoing gRPC metadata,
// replacing any values already present for key. It copies the metadata rather
// than writing through, since the map on ctx may be shared with other calls.
func withHeader(ctx context.Context, key, value string) context.Context {
	md, ok := metadata.FromOutgoingContext(ctx)
	if !ok {
		md = metadata.MD{}
	} else {
		md = md.Copy()
	}

	md.Set(key, value)
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
