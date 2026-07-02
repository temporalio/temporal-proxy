// Package interceptor provides gRPC client interceptors for the Temporal proxy.
//
// Use Payloads to build a [google.golang.org/grpc.UnaryClientInterceptor] that
// runs a chain of PayloadCodec implementations over every payload passing
// through the proxy: codecs encode in the order given on outbound requests and
// decode in reverse order on inbound responses.
package interceptor
