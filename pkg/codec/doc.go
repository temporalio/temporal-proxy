// Package codec converts Temporal payloads on their way to and from an upstream.
//
// [Encryptor] seals payloads with envelope encryption through a [Cipher] and opens
// them again. A sealed payload is self-describing: the ciphertext travels with
// the ID of the key that wrapped its DEK and the wrapped DEK itself, so opening
// one needs nothing but the payload and a Cipher that can reach that key.
//
// [NewChain] assembles the codecs its options enable into a single [Chain] in the
// order they have to be applied, so callers say what they want enabled rather
// than what order it happens in.
//
// A [Chain] satisfies the Temporal SDK's converter.PayloadCodec, so it can be
// handed to converter.NewCodecDataConverter for an SDK client, with the SDK
// imported at the call site rather than here. It also backs the proxy's own
// HTTP codec server (internal/codecserver), which answers the encode, decode,
// and download routes the Temporal CLI and Web UI use, superseding the SDK's
// own two-route converter.NewPayloadCodecHTTPHandler.
package codec
