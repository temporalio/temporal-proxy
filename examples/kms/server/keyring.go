package main

import (
	"context"
	"crypto/hkdf"
	"crypto/sha256"
	"errors"
	"fmt"

	"github.com/temporalio/temporal-proxy/pkg/ext"
)

const (
	// currentVersion is the version this provider reports as current. A real key
	// service answers that question itself, and rotates without anyone editing a
	// constant; this provider has no key service, so a constant is what it has.
	// Bumping it seals new payloads under a new key without stranding the ones
	// already sealed.
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

// Key answers a key lookup: it derives the wrapping key for the requested
// version and Namespace, and reports which version that was.
//
// It is the whole of this provider. [ext.NewKeyWrapper] does the sealing, the
// framing, and the nonce around it, so a real deployment replaces this one
// method with a call to its own key service and writes no more cryptography
// than this does.
//
// An empty [ext.KeyRequest.Version] asks for whichever key is current. A real
// key service answers that from its own state and rotates without anyone
// editing a constant; this provider has no key service, so currentVersion is
// what it answers with. Derivation is otherwise deterministic, so a restarted
// provider still opens key material sealed before the restart.
func (k *keyring) Key(_ context.Context, req ext.KeyRequest) (ext.Key, error) {
	version := req.Version
	if version == "" {
		version = currentVersion
	}

	info := fmt.Sprintf("%s/v%s/%s", infoPrefix, version, req.Namespace)

	key, err := hkdf.Key(sha256.New, k.secret, nil, info, keySize)
	if err != nil {
		return ext.Key{}, fmt.Errorf("server: failed to derive a key for namespace %q: %w", req.Namespace, err)
	}

	return ext.Key{Bytes: key, Version: version}, nil
}
