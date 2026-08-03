// Package outbound presents the proxy's own credentials on connections it dials.
//
// A [CredentialProvider] supplies per-RPC metadata for every call over a
// connection. [CredentialProviderFor] selects one from configuration, and
// [DialOptions] installs it, together with interceptors that strip the
// credential's header from each call's forwarded metadata so a value being
// relayed cannot collide with the credential.
//
// Headers are canonicalized to lowercase and default to "authorization" with a
// "Bearer" scheme, matching what gRPC does to metadata keys on the wire.
package outbound
