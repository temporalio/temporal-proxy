package codec

import (
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
)

type (
	// Encryptor seals payloads with envelope encryption and opens them again.
	Encryptor struct {
		cipher Cipher
	}

	// Cipher encrypts and decrypts bytes. It is the subset of a key-management
	// backend, typically a [crypto.Vault], that [Encryptor] depends on.
	Cipher interface {
		Encrypt([]byte) (*crypto.Message, error)
		Decrypt(*crypto.Message) ([]byte, error)
	}
)

// NewEncryptor returns an [Encryptor] that seals and opens payloads through c.
func NewEncryptor(c Cipher) *Encryptor {
	return &Encryptor{cipher: c}
}

// Encode seals every payload in payloads, returning payloads whose data is the
// ciphertext and whose metadata carries the wrapped DEK needed to open it. Each
// original payload is sealed whole, metadata included, so [Encryptor.Decode]
// restores it exactly.
func (c *Encryptor) Encode(payloads []*common.Payload) ([]*common.Payload, error) {
	res := make([]*common.Payload, len(payloads))
	for i, p := range payloads {
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
// same encoding name without our key material.
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
