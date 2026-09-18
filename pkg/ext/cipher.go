package ext

import (
	"crypto/aes"
	"crypto/cipher"
	"fmt"

	"golang.org/x/crypto/chacha20poly1305"

	extv1 "github.com/temporalio/temporal-proxy/pkg/api/ext/v1"
)

// keyBytes is the key size every built-in cipher takes. All three are 256-bit
// constructions, so a [KeyLookup] returns the same length whichever is selected
// and switching ciphers does not mean re-keying.
const keyBytes = 32

const (
	// CipherAES256GCM selects AES-256-GCM, the default and the same cipher the
	// proxy seals payloads with.
	CipherAES256GCM = extv1.KeyMaterial_CIPHER_AES_256_GCM

	// CipherChaCha20Poly1305 selects ChaCha20-Poly1305, which is worth preferring
	// where AES has no hardware support.
	CipherChaCha20Poly1305 = extv1.KeyMaterial_CIPHER_CHACHA20_POLY1305

	// CipherXChaCha20Poly1305 selects XChaCha20-Poly1305, whose 24-byte nonce is
	// wide enough that random nonces need no counting.
	CipherXChaCha20Poly1305 = extv1.KeyMaterial_CIPHER_XCHACHA20_POLY1305
)

type (
	// CipherID names the AEAD that sealed a piece of key material. It travels in
	// the material, so a server that changes cipher still opens what the previous
	// one sealed.
	CipherID = extv1.KeyMaterial_Cipher

	// CipherFunc builds an AEAD over a wrapping key. Register one with
	// [WithCipherFunc] to seal with a cipher this package does not ship.
	CipherFunc func(key []byte) (cipher.AEAD, error)
)

// NewAES256GCM returns AES-256-GCM over a 32-byte key.
func NewAES256GCM(key []byte) (cipher.AEAD, error) {
	if err := checkKeySize("AES-256-GCM", key); err != nil {
		return nil, err
	}

	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, fmt.Errorf("failed to create an AES cipher: %w", err)
	}

	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("failed to create AES-256-GCM: %w", err)
	}

	return aead, nil
}

// NewChaCha20Poly1305 returns ChaCha20-Poly1305 over a 32-byte key.
func NewChaCha20Poly1305(key []byte) (cipher.AEAD, error) {
	if err := checkKeySize("ChaCha20-Poly1305", key); err != nil {
		return nil, err
	}

	aead, err := chacha20poly1305.New(key)
	if err != nil {
		return nil, fmt.Errorf("failed to create ChaCha20-Poly1305: %w", err)
	}

	return aead, nil
}

// NewXChaCha20Poly1305 returns XChaCha20-Poly1305 over a 32-byte key.
func NewXChaCha20Poly1305(key []byte) (cipher.AEAD, error) {
	if err := checkKeySize("XChaCha20-Poly1305", key); err != nil {
		return nil, err
	}

	aead, err := chacha20poly1305.NewX(key)
	if err != nil {
		return nil, fmt.Errorf("failed to create XChaCha20-Poly1305: %w", err)
	}

	return aead, nil
}

// defaultCiphers returns the ciphers a [KeyWrapper] understands before any
// option is applied. Each call returns a fresh map, so applying options to one
// wrapper cannot reach another.
func defaultCiphers() map[CipherID]CipherFunc {
	return map[CipherID]CipherFunc{
		CipherAES256GCM:         NewAES256GCM,
		CipherChaCha20Poly1305:  NewChaCha20Poly1305,
		CipherXChaCha20Poly1305: NewXChaCha20Poly1305,
	}
}

// checkKeySize reports a wrong-sized key as the caller's problem rather than
// letting it surface as whatever the underlying constructor says, since the key
// came from a [KeyLookup] and naming the cipher is what makes that findable.
func checkKeySize(name string, key []byte) error {
	if len(key) != keyBytes {
		return fmt.Errorf("%s needs a %d-byte key, got %d", name, keyBytes, len(key))
	}

	return nil
}
