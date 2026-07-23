// Package kms wires the proxy's encryption configuration into a running
// [crypto.Vault].
//
// It reads [config.Encryption], opens the configured KMS keys as KEKs (via
// gocloud.dev/secrets, so any of awskms, azurekeyvault, gcpkms, or a local
// testing key is supported), assembles a [crypto.KEKRegistry], and constructs
// the [crypto.Vault] that the rest of the proxy uses to seal and open payloads.
//
// The package exposes a single [Module] for Uber fx. When encryption is
// disabled the module provides a nil *crypto.Vault and starts no background
// work; when enabled it also runs a goroutine that periodically refreshes the
// vault so DEKs rotate ahead of expiry.
package kms
