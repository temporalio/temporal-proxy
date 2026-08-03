// Package auth authenticates requests arriving at the proxy.
//
// An [Authenticator] decides whether a caller may proceed, and
// [StreamServerInterceptor] applies one to every inbound stream, rejecting a
// caller before the stream is routed and stripping the credential headers it
// consumed so they never reach an upstream.
//
// [Module] selects the authenticator from configuration: a fixed static token,
// OIDC/JWKS verification, or an extension server that decides on the proxy's
// behalf. Authentication is opt-in, so an absent auth block admits every
// request, but a block that selects nothing usable is an error rather than a
// silent return to admitting everyone.
//
// A refused caller is told only a generic status. The reason travels alongside it
// for the proxy's log, so a rejection is diagnosable without telling the caller
// which part of its credential failed.
package auth
