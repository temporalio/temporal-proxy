// Package router routes inbound gRPC traffic to the upstream that serves it.
//
// It provides the pieces that are wired onto the gateway:
//
//   - Codec returns a hybrid [google.golang.org/grpc/encoding.CodecV2] that
//     carries frames as raw bytes and delegates every other message to the
//     standard proto codec, so locally registered services (such as health)
//     keep working alongside routing.
//   - PeekInterceptor reads the namespace from a request's first message,
//     buffering that message so the handler that serves the stream still
//     receives it.
//   - Handler returns a [google.golang.org/grpc.StreamHandler] for use as a
//     [google.golang.org/grpc.UnknownServiceHandler]. It asks a Director for
//     the upstream and hands the stream to that upstream's handler, with the
//     resolved namespace as outgoing metadata.
//
// Together they let the server route any method it does not handle locally,
// selecting the upstream per request from the routing table MuxFor builds.
//
// internal/dataplane assembles these pieces directly: it calls MuxFor,
// Codec, PeekInterceptor, NewDirector, NewReporter, and Handler to build the
// request path.
package router
