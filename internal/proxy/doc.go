// Package proxy serves every allowed Temporal service on a local unix socket,
// forwarding each request to an upstream Temporal frontend over gRPC. The
// socket path is derived from the upstream host:port, so local workers connect
// without TLS while the upstream hop stays secured.
package proxy
