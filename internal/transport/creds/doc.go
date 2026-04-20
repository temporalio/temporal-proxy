// Package creds provides gRPC transport credential implementations for use
// with Temporal proxy connections.
//
// Each credential type exposes two methods:
//
//   - DialOption returns a [google.golang.org/grpc.DialOption] for configuring
//     outbound (client) connections.
//   - ServerOption returns a [google.golang.org/grpc.ServerOption] for
//     configuring inbound (server) connections.
//
// # Available Credential Types
//
// [Insecure] disables transport security entirely. Suitable for
// local development or environments where encryption is handled at another
// layer (e.g., a service mesh).
//
// [TLS] enables one-way TLS, authenticating the server to the client using a
// certificate/key pair. The client does not present a certificate.
//
// [MTLS] enables mutual TLS, requiring both sides to present and verify
// certificates. Use this when the proxy must authenticate to an upstream
// Temporal cluster that enforces client certificates, or when the proxy itself
// must verify connecting clients.
package creds
