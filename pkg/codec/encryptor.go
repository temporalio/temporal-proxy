package codec

import (
	"errors"
	"fmt"

	"go.temporal.io/api/common/v1"

	"github.com/temporalio/temporal-proxy/pkg/crypto"
)

// These keys form the on-the-wire contract for a sealed payload: the encoding
// marker lets Decode recognize its own output, and the key-ID and wrapped-DEK
// entries carry the material needed to open it.
const (
	// MetadataEncoding is the payload metadata key holding the encoding name.
	MetadataEncoding = "encoding"

	// MetadataEncryptionKeyID is the payload metadata key holding the ID of the
	// KEK that wrapped the DEK.
	MetadataEncryptionKeyID = "encryption-key-id"

	// MetadataEncryptionDEK is the payload metadata key holding the wrapped DEK.
	MetadataEncryptionDEK = "encryption-dek"

	// EncryptionEncoding is the encoding a sealed payload is marked with.
	EncryptionEncoding = "binary/encrypted"

	// SkipOpEncrypt is the op a SkipObserver is told when Encode forwards a
	// payload whose encoding is listed.
	SkipOpEncrypt = "encrypt"

	// SkipOpDecrypt is the op a SkipObserver is told when Decode passes through a
	// payload sealed under a KEK the cipher doesn't hold.
	SkipOpDecrypt = "decrypt"
)

type (
	// Encryptor seals payloads with envelope encryption and opens them again.
	Encryptor struct {
		cipher Cipher
		skip   map[string]struct{}
		onSkip SkipObserver
	}

	// EncryptorOption configures an [Encryptor].
	EncryptorOption func(*Encryptor)

	// SkipObserver is told about each payload an [Encryptor] leaves as it was:
	// op is SkipOpEncrypt or SkipOpDecrypt, and encoding is the payload's.
	SkipObserver func(op, encoding string)

	// Cipher encrypts and decrypts bytes. It is what [Encryptor] depends on,
	// typically a wrapper that binds a [crypto.Vault] to a namespace.
	Cipher interface {
		Encrypt([]byte) (*crypto.Message, error)
		Decrypt(*crypto.Message) ([]byte, error)
	}
)

// NewEncryptor returns an [Encryptor] that seals and opens payloads through c.
func NewEncryptor(c Cipher, opts ...EncryptorOption) *Encryptor {
	e := &Encryptor{cipher: c}
	for _, opt := range opts {
		opt(e)
	}

	return e
}

// WithSkipEncodings lists encodings Encode treats as already encrypted and
// forwards unchanged. Blank entries are dropped, since one would match every
// payload that carries no encoding. A later use replaces an earlier one.
func WithSkipEncodings(encodings ...string) EncryptorOption {
	set := make(map[string]struct{}, len(encodings))
	for _, enc := range encodings {
		if enc != "" {
			set[enc] = struct{}{}
		}
	}

	return func(e *Encryptor) { e.skip = set }
}

// WithSkipObserver reports each payload the [Encryptor] skips to fn. A nil fn
// reports nothing.
func WithSkipObserver(fn SkipObserver) EncryptorOption {
	return func(e *Encryptor) { e.onSkip = fn }
}

// Encode seals every payload in payloads, returning payloads whose data is
// the ciphertext and whose metadata carries the wrapped DEK needed to open
// it. Each original payload is sealed whole, metadata included, so
// [Encryptor.Decode] restores it exactly. Payloads whose encoding is listed
// through [WithSkipEncodings] are returned as they are.
func (c *Encryptor) Encode(payloads []*common.Payload) ([]*common.Payload, error) {
	res := make([]*common.Payload, len(payloads))
	for i, p := range payloads {
		if len(c.skip) > 0 {
			if enc := string(p.GetMetadata()[MetadataEncoding]); c.skips(enc) {
				res[i] = p
				c.skipped(SkipOpEncrypt, enc)
				continue
			}
		}

		data, err := p.Marshal()
		if err != nil {
			return nil, fmt.Errorf("failed to marshal payload: %w", err)
		}

		msg, err := c.cipher.Encrypt(data)
		if err != nil {
			return nil, fmt.Errorf("failed to encrypt payload: %w", err)
		}

		res[i] = &common.Payload{
			Metadata: map[string][]byte{
				MetadataEncoding:        []byte(EncryptionEncoding),
				MetadataEncryptionKeyID: []byte(msg.KeyMaterial.KEKID),
				MetadataEncryptionDEK:   []byte(msg.KeyMaterial.EncryptedDEK),
			},
			Data: msg.Ciphertext,
		}
	}

	return res, nil
}

// Decode reverses [Encryptor.Encode]: a payload carrying the full sealed-payload
// contract, the EncryptionEncoding marker plus both key-material entries, is opened
// and restored to its original form. Anything else passes through unchanged so
// payloads produced elsewhere survive the round trip, including ones that use the
// same encoding name without our key material. When EncryptionEncoding is listed
// through [WithSkipEncodings], a payload sealed under a KEK the cipher doesn't hold
// is passed through too.
func (c *Encryptor) Decode(payloads []*common.Payload) ([]*common.Payload, error) {
	res := make([]*common.Payload, len(payloads))
	for i, p := range payloads {
		// Only decrypt what we've encrypted
		if enc := string(p.Metadata[MetadataEncoding]); enc != EncryptionEncoding {
			res[i] = p
			continue
		}

		// The marker alone is not proof we sealed this: anything else using the
		// same encoding name would carry no key material of ours. Treat the full
		// set as the claim of ownership and pass through what doesn't make it.
		if len(p.Metadata[MetadataEncryptionKeyID]) == 0 || len(p.Metadata[MetadataEncryptionDEK]) == 0 {
			res[i] = p
			continue
		}

		pt, err := c.cipher.Decrypt(&crypto.Message{
			Ciphertext: p.Data,
			KeyMaterial: &crypto.DEKMaterial{
				KEKID:        string(p.Metadata[MetadataEncryptionKeyID]),
				EncryptedDEK: string(p.Metadata[MetadataEncryptionDEK]),
			},
		})
		if err != nil {
			// A chained hop sees payloads an earlier proxy sealed under keys it
			// doesn't hold. Listing EncryptionEncoding is the operator saying
			// those belong to someone else.
			if c.skips(EncryptionEncoding) && errors.Is(err, crypto.ErrUnknownKey) {
				res[i] = p
				c.skipped(SkipOpDecrypt, EncryptionEncoding)
				continue
			}

			return nil, fmt.Errorf("failed to decrypt payload: %w", err)
		}

		og := new(common.Payload)
		if err := og.Unmarshal(pt); err != nil {
			return nil, fmt.Errorf("failed to unmarshal payload: %w", err)
		}

		res[i] = og
	}

	return res, nil
}

// skips reports whether enc is listed through WithSkipEncodings.
func (c *Encryptor) skips(enc string) bool {
	_, ok := c.skip[enc]
	return ok
}

// skipped tells the observer, if there is one, that a payload was left alone.
func (c *Encryptor) skipped(op, enc string) {
	if c.onSkip != nil {
		c.onSkip(op, enc)
	}
}
