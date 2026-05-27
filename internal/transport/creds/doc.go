// Package creds provides gRPC transport credential implementations for use
// with Temporal proxy connections.
//
// Each credential type exposes two methods:
//
//   - DialOption returns a [google.golang.org/grpc.DialOption] for configuring
//     outbound (client) connections.
//   - ServerOption returns a [google.golang.org/grpc.ServerOption] for
//     configuring inbound (server) connections.
package creds
