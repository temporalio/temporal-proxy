// Package crypto implements envelope encryption.
//
// Data is encrypted with a Data Encryption Key ([DEK]) using AES-256-GCM. Each
// DEK is in turn encrypted ("wrapped") by a Key Encryption Key ([KEK]),
// typically customer-managed and backed by a cloud KMS. The wrapped DEK and the
// ID of the KEK that wrapped it are carried together as [DEKMaterial], allowing
// the DEK to be recovered and the data decrypted later.
//
// A [KEKRegistry] manages the set of available KEKs, selecting the appropriate
// key by namespace for encryption and by key ID for decryption. KEKs themselves
// are opened from key URIs by a [KeyFactory], which handles the cloud KMS
// schemes directly and can be extended with schemes of the caller's own.
package crypto
