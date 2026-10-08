// Package httpserver serves every HTTP route group, such as the codec server,
// on one port separate from the gateway's.
//
// A route group contributes its handlers as [Route] values through
// [AsRoutes], and owns everything specific to its callers, including CORS and
// authentication. This package owns only the listener: its address, TLS, and
// lifecycle. With no group enabled, nothing binds.
//
// A [Server] is single-use and is not restartable after [Server.Stop].
package httpserver
