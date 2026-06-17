// Package connect manages a pool of reusable gRPC client connections keyed by
// host. Use NewPool to create a Pool, Set to register a connection for a host,
// Conn to retrieve it, and Close to shut every connection down exactly once.
package connect
