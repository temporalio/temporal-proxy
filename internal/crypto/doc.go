// Package crypto implements envelope encryption for Temporal payload data.
//
// Envelope encryption uses two layers of keys:
//
//   - A [DEK] (Data Encryption Key) encrypts the actual payload using AES-256-GCM.
//   - A [KEK] (Key Encryption Key) encrypts the DEK. KEKs are customer-managed and
//     are typically backed by an external KMS such as AWS KMS or GCP Cloud KMS.
//
// A [KEKRegistry] maps Temporal namespaces to their KEKs. To encrypt a DEK, call
// [KEKRegistry.Encrypt] with the target namespace; to recover a DEK, call
// [KEKRegistry.Decrypt] with the [DEKMaterial] returned from a prior encrypt call.
package crypto
