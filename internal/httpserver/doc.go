// Package httpserver serves every HTTP route group, such as the codec server,
// on one port separate from the gateway's.
//
// A route group contributes its handlers as [Route] values through
// [AsRoutes], and owns everything specific to its callers, including CORS and
// authentication. This package owns the listener (its address, TLS, and
// lifecycle) and one route of its own, /healthz, which reports on the listener
// and nothing behind it. That route is unauthenticated, so a load balancer can
// probe the port without holding a group's credential. With no group enabled,
// nothing binds.
//
// A [Server] is single-use and is not restartable after [Server.Stop].
package httpserver
