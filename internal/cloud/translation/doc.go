// Package translation rewrites one gRPC method call into another on the hop to
// the upstream.
//
// A [Translation] pairs an inbound method with the upstream method that stands
// in for it, plus the conversions between their request and response types. A
// [Registry] holds the set of them, and [DialOptions] installs them on a
// connection: a call whose method is registered is converted, sent under the
// upstream method, and converted back before the caller sees a reply.
// Everything else is passed straight through.
//
// It lives under internal/cloud because Temporal Cloud is what needs it - Cloud
// serves some methods only from its control plane, under another service - and
// because "translation" unqualified already means namespace translation
// elsewhere in the proxy. That one rewrites names inside a message and leaves
// the method alone; this one replaces the call. The two compose: install this
// innermost, so namespace translation, payload codecs, and the reflective
// forwarder all keep seeing the method and message types the caller asked for.
//
// The one translation the proxy ships is WorkflowService.ListNamespaces onto
// CloudService.GetNamespaces, since Temporal Cloud serves the namespace list
// from its own control plane rather than from a frontend.
//
// The mechanism itself knows nothing about Cloud, and is kept separate from the
// parent package so that using [cloud.IsEndpoint] or [cloud.ValidateNamespace]
// does not pull gRPC and protobuf into a caller that only wanted to check a
// name.
package translation
