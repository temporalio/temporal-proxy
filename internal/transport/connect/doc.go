// Package connect manages a pool of reusable gRPC client connections keyed by
// a caller-supplied logical key, distinct from the dial target. Use NewPool to
// create a Pool, Set to register a connection for a key, Conn to retrieve it,
// ConnOrCreate to retrieve or create one on first use, and Close to shut every connection
// down exactly once.
//
// Conn wraps that pool as a [grpc.ClientConnInterface] whose dial target is
// chosen per call by a Resolver: StaticResolver for a fixed upstream, or a
// caller-supplied dynamic Resolver that varies the target with the request.
package connect
