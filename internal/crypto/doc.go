// Package crypto implements envelope encryption for Temporal payload data.
//
// Callers should use [Sealer], which manages per-namespace DEK rotation and
// provides [Sealer.Seal] / [Sealer.Open] for encrypting and decrypting payloads.
//
// Envelope encryption uses two layers of keys:
//
//   - A [DEK] (Data Encryption Key) encrypts the actual payload using AES-256-GCM.
//   - A [KEK] (Key Encryption Key) encrypts the DEK. KEKs are customer-managed and
//     are typically backed by an external KMS such as AWS KMS or GCP Cloud KMS.
//
// A [KEKRegistry] maps namespaces to their KEKs and is used internally by
// [Sealer] to wrap and unwrap DEKs during seal and open operations.
package crypto
