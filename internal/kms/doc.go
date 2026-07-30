// Package kms wires the proxy's encryption configuration into a running
// [crypto.Vault].
//
// It reads [config.Encryption], opens the configured keys as KEKs, assembles a
// [crypto.KEKRegistry], and constructs the [crypto.Vault] that the rest of the
// proxy uses to seal and open payloads.
//
// Keys are opened by a [KeyFactory], which extends [crypto.KeyFactory] in two
// ways. It adds the "extension://" scheme, resolving such a key to an
// operator-run extension server over a connection supplied by the api package;
// several keys may name the same server, so each is identified by its whole URI.
// Every other scheme is a cloud KMS key that crypto opens through
// gocloud.dev/secrets (awskms, azurekeyvault, gcpkms, or a local testing key).
// It also meters what it opens, so each key's wraps and unwraps are recorded
// against its provider.
//
// The package exposes a single [Module] for Uber fx. When encryption is
// disabled the module provides a nil *crypto.Vault and starts no background
// work; when enabled it also runs a goroutine that periodically refreshes the
// vault so DEKs rotate ahead of expiry.
package kms
