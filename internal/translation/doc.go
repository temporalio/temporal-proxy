// Package translation rewrites one gRPC method call into another on the hop to
// the upstream.
//
// A [Translation] pairs an inbound method with the upstream method that stands
// in for it, plus the conversions between their request and response types. A
// [Registry] holds the set of them, and [UnaryClientInterceptor] applies it:
// a call whose method is registered is converted, sent under the upstream
// method, and converted back before the caller sees a reply. Everything else is
// passed straight through.
//
// This is distinct from the namespace translation in internal/proxy, which
// rewrites names inside a message but leaves the method alone. The two compose:
// install method translation as the innermost interceptor so namespace
// translation, payload codecs, and the reflective forwarder all keep seeing the
// method and message types the caller actually asked for.
//
// The one translation the proxy ships is WorkflowService.ListNamespaces onto
// CloudService.GetNamespaces, since Temporal Cloud serves the namespace list
// from its own control plane rather than from a frontend.
package translation
