package main

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/hkdf"
	"crypto/rand"
	"crypto/sha256"
	"errors"
	"fmt"

	extv1 "github.com/temporalio/temporal-proxy/pkg/api/ext/v1"
)

const (
	// currentVersion is the wrapping key version Wrap stamps into new key
	// material. Unwrap derives from whatever version it is handed, so bumping
	// this rotates new payloads without stranding the ones already sealed.
	currentVersion = "1"

	// keySize selects AES-256.
	keySize = 32

	// infoPrefix domain-separates these derived keys from any other use of the
	// same master secret.
	infoPrefix = "temporal-proxy-kek"
)

// keyring derives one wrapping key per version and namespace from a master
// secret, so a compromise of one namespace's key does not hand over the others.
type keyring struct {
	secret []byte
}

// newKeyring returns a keyring over secret. An empty secret is an error rather
// than a silently weak key.
//
// secret should be at least 32 bytes of cryptographic randomness (for example
// from "openssl rand -base64 32"), not a human-chosen passphrase: HKDF extracts
// entropy from secret rather than adding any, so a guessable secret makes every
// derived per-namespace key guessable too. newKeyring only checks that secret is
// non-empty; it cannot tell a random secret from a memorable one.
func newKeyring(secret []byte) (*keyring, error) {
	if len(secret) == 0 {
		return nil, errors.New("server: master secret is required")
	}

	return &keyring{secret: secret}, nil
}

// Wrap seals dek under the key derived for namespace and returns it as
// [extv1.KeyMaterial], which carries everything Unwrap needs to derive the same
// key again.
//
// The namespace travels in the clear, and doubles as the GCM additional data so
// key material relabelled with another namespace fails to open. A namespace is
// not a secret (it already travels in gRPC metadata), but a provider that would
// rather not expose one can leave the field empty and identify its key through
// opaque instead.
func (k *keyring) Wrap(_ context.Context, namespace string, dek []byte) ([]byte, error) {
	gcm, err := k.cipher(currentVersion, namespace)
	if err != nil {
		return nil, err
	}

	nonce := make([]byte, gcm.NonceSize())
	// crypto/rand.Read never returns an error; it crashes the program instead.
	_, _ = rand.Read(nonce)

	material := &extv1.KeyMaterial{
		EncryptedDek: gcm.Seal(nil, nonce, dek, []byte(namespace)),
		Version:      currentVersion,
		Namespace:    namespace,
		// KeyMaterial has no field for a nonce, and needs none: opaque is where a
		// server puts whatever its own wrapping requires. A nonce is not secret,
		// only single-use.
		Opaque: nonce,
	}

	packed, err := material.Marshal()
	if err != nil {
		return nil, fmt.Errorf("server: failed to pack key material: %w", err)
	}

	return packed, nil
}

// Unwrap opens key material produced by Wrap, deriving the key from the version
// and namespace it carries: the two together address one key, the way a lookup
// against a real key service would.
func (k *keyring) Unwrap(_ context.Context, ciphertext []byte) ([]byte, error) {
	material, err := extv1.UnmarshalKeyMaterial(ciphertext)
	if err != nil {
		return nil, fmt.Errorf("server: %w", err)
	}

	// Unmarshal accepts bytes that set no fields at all, so anything arriving
	// from outside gets checked before it is trusted.
	if err := material.Validate(); err != nil {
		return nil, fmt.Errorf("server: invalid key material: %w", err)
	}

	gcm, err := k.cipher(material.GetVersion(), material.GetNamespace())
	if err != nil {
		return nil, err
	}

	nonce := material.GetOpaque()
	if len(nonce) != gcm.NonceSize() {
		return nil, fmt.Errorf("server: key material carries a %d-byte nonce, want %d", len(nonce), gcm.NonceSize())
	}

	dek, err := gcm.Open(nil, nonce, material.GetEncryptedDek(), []byte(material.GetNamespace()))
	if err != nil {
		return nil, fmt.Errorf("server: failed to open key material for namespace %q: %w", material.GetNamespace(), err)
	}

	return dek, nil
}

// cipher derives the wrapping key for version and namespace and returns a GCM
// cipher over it. Derivation is deterministic, so a restarted provider still
// opens key material sealed before the restart.
func (k *keyring) cipher(version, namespace string) (cipher.AEAD, error) {
	info := fmt.Sprintf("%s/v%s/%s", infoPrefix, version, namespace)

	key, err := hkdf.Key(sha256.New, k.secret, nil, info, keySize)
	if err != nil {
		return nil, fmt.Errorf("server: failed to derive a key for namespace %q: %w", namespace, err)
	}

	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, fmt.Errorf("server: failed to create cipher: %w", err)
	}

	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("server: failed to create GCM: %w", err)
	}

	return gcm, nil
}
