// Package connect manages a pool of reusable gRPC client connections keyed by
// host. Use NewPool to create a Pool, Set to register a connection for a host,
// Conn to retrieve it, GetOrSet to retrieve or lazily dial one, and Close to
// shut every connection down exactly once.
package connect
