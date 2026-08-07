// Package rpc holds the gRPC stream plumbing the proxy's forwarding paths share.
//
// [ServiceMethod], [Service], and [Method] split a gRPC full method name into
// the halves an allowlist check or a descriptor lookup needs. [Pump.Forward]
// forwards one call between an inbound server stream and an outbound client
// stream, and [StatusError] maps a failure along the way to the status the caller
// sees.
package rpc
