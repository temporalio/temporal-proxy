// Package creds resolves TLS transport credentials for Temporal proxy
// connections from a small set of options.
//
// A [Dialer] secures outbound (client) connections and a [Listener] secures
// inbound (server) connections. Both are built from the same options
// ([Insecure], [WithCA], [WithCertificate]) but interpret them per role: for a
// client a CA is a trust anchor and a certificate is presented to the upstream;
// for a server a CA requires and verifies client certificates. Each constructor
// resolves the TLS [Mode] and the cross-field legality of the configuration
// once, so validation, dialing, and serving cannot disagree.
//
// Security is the default: only [Insecure] yields a plaintext credential, and an
// accidentally-empty client credential verifies the peer against the system root
// pool rather than silently downgrading. Construction performs no file I/O;
// certificate material is read and parsed lazily when a credential is validated
// or first used. It is loaded once and then refreshed by a Reloader when one is
// attached. Every TLS handshake builds its configuration from the material in
// use at that moment, so new connections pick up a reload while existing ones
// keep the material they negotiated with.
package creds
