// Package router transparently forwards inbound gRPC traffic to an upstream
// connection without decoding it.
//
// It provides two pieces that are wired onto the inbound server:
//
//   - Codec returns a hybrid [google.golang.org/grpc/encoding.CodecV2] that
//     passes relayed frames through as raw bytes and delegates every other
//     message to the standard proto codec, so locally registered services
//     (such as health) keep working alongside the relay.
//   - Handler returns a [google.golang.org/grpc.StreamHandler] for use as a
//     [google.golang.org/grpc.UnknownServiceHandler]. It opens a same-method
//     stream on the upstream connection and pumps raw frames in both
//     directions, propagating header, trailer, and status verbatim.
//
// Together they let the server forward any method it does not handle locally,
// with no knowledge of the underlying protobuf messages, selecting the
// upstream connection per request from the routing table CompileMux compiles.
//
// internal/dataplane assembles these pieces directly: it calls CompileMux,
// Codec, NewDirector, NewReporter, and Handler to build the request path.
package router
