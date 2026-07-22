package crypto

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"slices"
	"sync"
)

type (
	// KEK defines an interface for a Key Encryption Keys.
	// These keys are used to encrypt/decrypt DEKs and are customer-managed (e.g. via AWS/GCP KMS).
	KEK interface {
		io.Closer

		// ID returns a unique ID for this KEK, e.g. a KMS ARN.
		ID() string
		// Encrypt encrypts a DEK, returning the ciphertext.
		Encrypt(context.Context, []byte) ([]byte, error)
		// Decrypt decrypts a DEK previously produced by Encrypt.
		Decrypt(context.Context, []byte) ([]byte, error)
	}

	// KEKRegistry holds the set of KEKs available for encrypting and decrypting DEKs.
	// It is keyed by namespace (for encryption) and by key ID (for decryption).
	// Close must be called when the registry is no longer needed to release KEK resources.
	KEKRegistry struct {
		defaultKey      KEK            // Fallback when no namespace key exists.
		keks            map[string]KEK // map from NS -> KEK
		keyIDs          map[string]KEK // map from ID -> KEK
		decryptOnlyKeys []KEK          // used for decryption only, never selected for new encryption

		closeOnce sync.Once
		closeErr  error
	}

	// KEKRegistryOption configures a KEKRegistry during construction.
	KEKRegistryOption interface {
		apply(*KEKRegistry) error
	}

	kekRegOpt func(*KEKRegistry) error
)

// NewKEKRegistry constructs a KEKRegistry, applying opts in order. A default key is
// required (see [WithDefaultKey]); construction fails if one is not provided. The
// key-ID index used by Decrypt is built after all options are applied.
func NewKEKRegistry(opts ...KEKRegistryOption) (*KEKRegistry, error) {
	r := &KEKRegistry{
		keks: map[string]KEK{},
	}
	for _, opt := range opts {
		if err := opt.apply(r); err != nil {
			return nil, err
		}
	}

	if r.defaultKey == nil {
		return nil, errors.New("a default key is required")
	}

	// Ensure no duplicate keys between default/namespace/decrypt-only.
	idMap := make(map[string]KEK, len(r.keks)+len(r.decryptOnlyKeys)+1)
	add := func(k KEK) error {
		if _, ok := idMap[k.ID()]; ok {
			return fmt.Errorf("duplicate key id: %s", k.ID())
		}

		idMap[k.ID()] = k
		return nil
	}

	if err := add(r.defaultKey); err != nil {
		return nil, err
	}

	for _, v := range r.keks {
		if err := add(v); err != nil {
			return nil, err
		}
	}

	for _, k := range r.decryptOnlyKeys {
		if err := add(k); err != nil {
			return nil, err
		}
	}

	r.keyIDs = idMap
	return r, nil
}

// WithDefaultKey sets the fallback KEK used when no namespace-specific key is registered.
// A default key is required: NewKEKRegistry returns an error if one is not provided.
func WithDefaultKey(k KEK) KEKRegistryOption {
	return kekRegOpt(func(r *KEKRegistry) error {
		if k == nil {
			return errors.New("default key must not be nil")
		}

		r.defaultKey = k
		return nil
	})
}

// WithKeyForNamespace registers k for ns, used when encrypting or decrypting DEKs for that namespace.
func WithKeyForNamespace(ns string, k KEK) KEKRegistryOption {
	return kekRegOpt(func(r *KEKRegistry) error {
		if k == nil {
			return fmt.Errorf("key for namespace '%s' must not be nil", ns)
		}
		if _, ok := r.keks[ns]; ok {
			return fmt.Errorf("key for namespace '%s' already defined", ns)
		}

		r.keks[ns] = k
		return nil
	})
}

// WithDecryptOnlyKey registers k for decryption only. It is added to the key-ID index so that DEKs
// encrypted with k can still be opened, but k is never selected for new DEK encryption. This is
// typically used for keys that have been rotated out of active use.
func WithDecryptOnlyKey(k KEK) KEKRegistryOption {
	return kekRegOpt(func(r *KEKRegistry) error {
		if k == nil {
			return errors.New("decrypt-only key must not be nil")
		}

		if slices.ContainsFunc(r.decryptOnlyKeys, func(existing KEK) bool {
			return existing.ID() == k.ID()
		}) {
			return fmt.Errorf("key already defined: %s", k.ID())
		}

		r.decryptOnlyKeys = append(r.decryptOnlyKeys, k)
		return nil
	})
}

// Encrypt encrypts the given DEK using the KEK registered for the specified namespace.
// It returns DEKMaterial containing the KEK ID and the base64-encoded ciphertext.
func (r *KEKRegistry) Encrypt(ctx context.Context, ns string, dek *DEK) (*DEKMaterial, error) {
	if dek == nil {
		return nil, errors.New("dek must not be nil")
	}

	k, ok := r.keks[ns]
	if !ok {
		k = r.defaultKey
	}

	ct, err := k.Encrypt(ctx, dek.key)
	if err != nil {
		return nil, fmt.Errorf("failed to encrypt message in namespace: %s, %w", ns, err)
	}

	return &DEKMaterial{
		Version:      materialVersion,
		KEKID:        k.ID(),
		EncryptedDEK: base64.StdEncoding.EncodeToString(ct),
	}, nil
}

// Decrypt decrypts the DEK described by m using the KEK identified by m.KEKID.
func (r *KEKRegistry) Decrypt(ctx context.Context, m *DEKMaterial) (*DEK, error) {
	if m == nil {
		return nil, errors.New("dek material must not be nil")
	}

	k, ok := r.keyIDs[m.KEKID]
	if !ok {
		return nil, fmt.Errorf("unknown key: %s", m.KEKID)
	}

	ct, err := base64.StdEncoding.DecodeString(m.EncryptedDEK)
	if err != nil {
		return nil, fmt.Errorf("failed to decode DEK: %s, %w", m.KEKID, err)
	}

	key, err := k.Decrypt(ctx, ct)
	if err != nil {
		return nil, fmt.Errorf("failed to decrypt using KEK: %s, %w", m.KEKID, err)
	}

	if len(key) != keyBytes {
		return nil, fmt.Errorf("invalid DEK for KEK: %s", m.KEKID)
	}

	return dekFromKey(key)
}

// Close closes all registered KEKs and releases their resources.
// Subsequent calls return the same error as the first call.
func (r *KEKRegistry) Close() error {
	// Blocking concurrent callers here is acceptable: Close is a shutdown
	// operation; callers should not race to close, and if they do, waiting
	// for a single authoritative result is the right behaviour.
	r.closeOnce.Do(func() {
		// keyIDs holds the default, namespace, and decrypt-only keys deduped by ID,
		// so iterating it closes every distinct KEK exactly once.
		errs := make([]error, 0, len(r.keyIDs))
		for id, kek := range r.keyIDs {
			if err := kek.Close(); err != nil {
				errs = append(errs, fmt.Errorf("failed to close KEK: %s, %w", id, err))
			}
		}
		r.closeErr = errors.Join(errs...)
	})

	return r.closeErr
}

func (f kekRegOpt) apply(r *KEKRegistry) error {
	return f(r)
}
