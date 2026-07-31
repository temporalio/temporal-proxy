// Package api reaches the extension servers an operator runs: gRPC services
// implementing one of the contracts published under api/, currently
// api.kms.v1.EncryptionService and api.auth.v1.AuthService.
//
// Extension servers exist so an operator can plug in a backend the proxy has no
// built-in support for, such as an on-prem HSM or an internal key service for
// encryption, or a policy engine or session service for authentication.
//
// [Module] dials every server named in the configuration and publishes the
// results as [Connections], keyed by server name. Callers build their own
// clients over those connections rather than receiving finished ones, because
// the two do not correspond one-to-one: [KMS] is the client for the encryption
// service, and one is built per key, several of which may live on one server.
// [Auth] is the client for the authentication service, and there is at most one
// of those, since the proxy admits a caller on a single verdict.
//
// Dialing happens when the connections are first demanded rather than on the
// first call over them, so a bad address, certificate, or credential is caught
// during construction. The connections outlive this package and are closed with
// the application, not by any client built over them.
package api
