package main

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/hkdf"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
)

const (
	// formatVersion prefixes every ciphertext so a later change to the framing is
	// rejected rather than mis-parsed.
	formatVersion = 0x01

	// headerSize is the fixed part of the frame: the version byte plus the uint16
	// namespace length that follows it.
	headerSize = 3

	// keySize selects AES-256.
	keySize = 32

	// infoPrefix domain-separates these derived keys from any other use of the
	// same master secret.
	infoPrefix = "temporal-proxy-kek/v1/"
)

// keyring derives one wrapping key per namespace from a master secret, so a
// compromise of one namespace's key does not hand over the others.
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

// wrap seals dek under the key derived for namespace.
//
// The namespace is written into the frame in the clear because unwrap is handed
// nothing but ciphertext and has to derive the same key again. It doubles as the
// GCM additional data, so a ciphertext relabelled with another namespace fails
// to open. A namespace is not a secret (it already travels in gRPC metadata),
// but a provider that would rather not expose one should carry an opaque key
// identifier here and resolve it internally.
func (k *keyring) wrap(namespace string, dek []byte) ([]byte, error) {
	if len(namespace) > math.MaxUint16 {
		return nil, fmt.Errorf("server: namespace is too long to frame: %d bytes", len(namespace))
	}

	gcm, err := k.cipher(namespace)
	if err != nil {
		return nil, err
	}

	out := make([]byte, 0, headerSize+len(namespace)+gcm.NonceSize()+len(dek)+gcm.Overhead())
	out = append(out, formatVersion)
	out = binary.BigEndian.AppendUint16(out, uint16(len(namespace)))
	out = append(out, namespace...)

	nonce := make([]byte, gcm.NonceSize())
	// crypto/rand.Read never returns an error; it crashes the program instead.
	_, _ = rand.Read(nonce)
	out = append(out, nonce...)

	return gcm.Seal(out, nonce, dek, []byte(namespace)), nil
}

// unwrap opens a ciphertext produced by wrap, deriving the key from the
// namespace the frame carries.
func (k *keyring) unwrap(ciphertext []byte) ([]byte, error) {
	if len(ciphertext) < headerSize {
		return nil, errors.New("server: ciphertext is too short to hold a header")
	}

	if ciphertext[0] != formatVersion {
		return nil, fmt.Errorf("server: unsupported ciphertext version: %#x", ciphertext[0])
	}

	nsLen := int(binary.BigEndian.Uint16(ciphertext[1:headerSize]))
	if len(ciphertext) < headerSize+nsLen {
		return nil, errors.New("server: ciphertext is truncated inside its namespace")
	}

	namespace := string(ciphertext[headerSize : headerSize+nsLen])
	sealed := ciphertext[headerSize+nsLen:]

	gcm, err := k.cipher(namespace)
	if err != nil {
		return nil, err
	}

	if len(sealed) < gcm.NonceSize() {
		return nil, errors.New("server: ciphertext is truncated inside its nonce")
	}

	dek, err := gcm.Open(nil, sealed[:gcm.NonceSize()], sealed[gcm.NonceSize():], []byte(namespace))
	if err != nil {
		return nil, fmt.Errorf("server: failed to open ciphertext for namespace %q: %w", namespace, err)
	}

	return dek, nil
}

// cipher derives the wrapping key for namespace and returns a GCM cipher over
// it. Derivation is deterministic, so a restarted provider still opens
// ciphertexts sealed before the restart.
func (k *keyring) cipher(namespace string) (cipher.AEAD, error) {
	key, err := hkdf.Key(sha256.New, k.secret, nil, infoPrefix+namespace, keySize)
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
