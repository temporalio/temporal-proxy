// Package proxy serves the Temporal WorkflowService on a local unix socket,
// forwarding every request to an upstream Temporal frontend over gRPC. The
// socket path is derived from the upstream host:port, so local workers connect
// without TLS while the upstream hop stays secured.
package proxy
