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
// skips the second admits anyone who finds its port.
//
// The proxy fails closed, so any error from either service denies the request.
// Return a [google.golang.org/grpc/status] error: the code tells the proxy
// whether the caller can fix this or should retry, and a plain error arrives as
// Unknown.
//
// [Serve] listens in plaintext unless [WithServerOption] supplies credentials.
// Both are supported, but the ends must agree, since the proxy dials plaintext
// when the extension server's TLS block is absent.
package ext
