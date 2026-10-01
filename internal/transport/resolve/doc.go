// Package resolve provides gRPC name resolvers. NewSRVBuilder handles srv:///
// targets: it reads the named DNS SRV record, balances round-robin across every
// backend it lists, and re-reads it on a fixed interval so membership changes are
// picked up. Pass the builder to grpc.NewClient with grpc.WithResolvers.
package resolve
