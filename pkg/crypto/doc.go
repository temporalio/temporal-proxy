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
//
// A [Vault] reports what it does to an [Observer] as one of a closed set of
// [Event] types: a [CacheEvent] for a DEK cache hit or miss, one
// [EnvelopeEvent] per Seal and Open carrying both the end-to-end and the
// AES-only duration and distinguishing a failure inside AES from a failure
// elsewhere in the operation, and a [RotationEvent] whenever a namespace's DEK
// is replaced. Observers are always called outside the Vault's internal lock,
// so an implementation may call back into the Vault, but it must not block,
// and it must not re-enter the operation that produced the event: calling
// Seal from Observe, for example, recurses without bound and a stack overflow
// cannot be recovered from.
package crypto
