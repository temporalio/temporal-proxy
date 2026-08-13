// Package ext is a starting point for building a temporal-proxy extension
// server.
//
// The proxy delegates two decisions to servers the operator runs: whether an
// inbound caller may proceed (api.auth.v1.AuthService) and how a data encryption
// key is wrapped (api.kms.v1.EncryptionService). Implement [Auth], [KMS], or both
// and hand them to [Serve] for the listener, signal handling, and graceful
// shutdown. Registering the generated services by hand works as well; nothing
// here is privileged. Both are registered either way, and an unsupplied one
// answers Unimplemented rather than refusing the connection.
//
// Two unrelated authentication decisions meet here and are easy to conflate.
// [Auth] is the proxy asking about a worker that connected to the gateway;
// [WithServerAuth] is about the caller of this server, which is the proxy.
// Answering the first does not imply enforcing the second, and a server that
// skips the second admits anyone who finds its port to everything but health.
//
// The gRPC health service is always registered, and always exempt from
// [WithServerAuth], since a probe has no credential to present. It reports SERVING
// from startup until shutdown begins, which makes it a liveness signal and not a
// readiness one: it says this process is up and answering, never that the [Auth]
// or [KMS] behind it can reach whatever it depends on. Wiring it to a readiness
// probe would report a server with an unreachable key store as ready.
//
// The proxy fails closed, so any error from either service denies the request.
// Return a [google.golang.org/grpc/status] error: the code tells the proxy
// whether the caller can fix this or should retry, and a plain error arrives as
// Unknown.
//
// [Allow], [Deny], [BearerToken], and [IsHealthCheckMethod] cover the parts of an
// [Auth] implementation that are the same everywhere, and the first two are worth
// preferring to a hand-built response: an [api.auth.v1.AuthResponse] whose
// Decision is unset denies, so building one by hand can refuse a caller by
// omission.
//
// [Serve] listens in plaintext unless [WithServerOption] supplies credentials, and
// warns once when the first call confirms it. Both are supported, but the ends must
// agree, since the proxy dials plaintext when the extension server's TLS block is
// absent.
package ext
