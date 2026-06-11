package crypto

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"errors"
	"fmt"
)

const (
	keyBytes        = 32 // 256 bits
	materialVersion = byte(1)
)

// ErrMalformedCipherText indicates that the ciphertext was not created by a call
// to Encrypt, or that it was otherwise tampered with.
var ErrMalformedCipherText = errors.New("invalid ciphertext, not encrypted with a DEK")

type (
	// DEK defines a Data Encryption Key used to encrypt/decrypt payloads.
	DEK struct {
		key []byte
		gcm cipher.AEAD
	}

	// DEKMaterial defines the material needed in order to decrypt a payload.
	DEKMaterial struct {
		Version      byte
		KEKID        string // The ID/URI of the KEK the encrypted the DEK.
		EncryptedDEK string // The base64-encoded encrypted DEK.
	}
)

// NewDEK generates a new random 256-bit Data Encryption Key.
func NewDEK() (*DEK, error) {
	k := make([]byte, keyBytes)
	if _, err := rand.Read(k); err != nil {
		return nil, fmt.Errorf("failed to create DEK: %w", err)
	}

	return dekFromKey(k)
}

// Encrypt encrypts the plaintext pt using AES-256-GCM. The returned ciphertext
// is prefixed with the randomly generated nonce.
func (d *DEK) Encrypt(ctx context.Context, pt []byte) ([]byte, error) {
	nonce := make([]byte, d.gcm.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return nil, fmt.Errorf("failed to generate nonce: %w", err)
	}

	return d.gcm.Seal(nonce, nonce, pt, nil), nil
}

// Decrypt decrypts the ciphertext ct using AES-256-GCM. The ciphertext must be
// prefixed with the nonce, as produced by [DEK.Encrypt].
func (d *DEK) Decrypt(ctx context.Context, ct []byte) ([]byte, error) {
	ns := d.gcm.NonceSize()
	if len(ct) < ns {
		return nil, ErrMalformedCipherText
	}

	pt, err := d.gcm.Open(nil, ct[:ns], ct[ns:], nil)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrMalformedCipherText, err)
	}

	return pt, nil
}

func dekFromKey(key []byte) (*DEK, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, fmt.Errorf("failed to create cipher block: %w", err)
	}

	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("failed to create cipher: %w", err)
	}

	return &DEK{key: key, gcm: gcm}, nil
}
