package crypto

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"sync"
)

var ErrNamespaceAlreadyExists = errors.New("key for namespace already defined")

type (
	// KEK defines an interface for Key Encryption Keys.
	// These keys are used to encrypt/decrypt DEKs and are customer-managed (e.g. via AWS/GCP KMS).
	KEK interface {
		io.Closer

		ID() string // A unique ID for this KEK, e.g. KMS ARN
		Encrypt(context.Context, []byte) ([]byte, error)
		Decrypt(context.Context, []byte) ([]byte, error)
	}

	// KEKRegistry holds the set of KEKs available for encrypting and decrypting DEKs.
	// It is keyed by namespace (for encryption) and by key ID (for decryption).
	// Close must be called when the registry is no longer needed to release KEK resources.
	KEKRegistry struct {
		defaultKey KEK            // Fallback when no namespace key exists.
		keks       map[string]KEK // map from NS -> KEK
		keyIDs     map[string]KEK // map from ID -> KEK

		closeOnce sync.Once
		closeErr  error
	}

	// KEKRegistryOption configures a KEKRegistry during construction.
	KEKRegistryOption interface {
		apply(*KEKRegistry)
	}

	kekRegOpt func(*KEKRegistry)

	// nilKEK defines a KEK that does nothing.
	nilKEK struct{}
)

// NewKEKRegistry constructs a KEKRegistry, applying opts in order.
// The key-ID index used by Decrypt is built after all options are applied.
func NewKEKRegistry(opts ...KEKRegistryOption) *KEKRegistry {
	r := &KEKRegistry{
		defaultKey: new(nilKEK),
		keks:       map[string]KEK{},
	}
	for _, opt := range opts {
		opt.apply(r)
	}

	idMap := make(map[string]KEK, len(r.keks))
	for _, v := range r.keks {
		idMap[v.ID()] = v
	}

	r.keyIDs = idMap
	return r
}

// WithDefaultKey sets the fallback KEK used when no namespace-specific key is registered.
func WithDefaultKey(k KEK) KEKRegistryOption {
	return kekRegOpt(func(r *KEKRegistry) {
		if k != nil { // nil means fallback to nilKEK.
			r.defaultKey = k
		}
	})
}

// WithKeyForNamespace registers k for ns, used when encrypting or decrypting DEKs for that namespace.
func WithKeyForNamespace(ns string, k KEK) KEKRegistryOption {
	return kekRegOpt(func(r *KEKRegistry) {
		if _, ok := r.keks[ns]; ok {
			panic(fmt.Sprintf("Key for namespace '%s' already defined", ns))
		}

		r.keks[ns] = k
	})
}

// Encrypt encrypts the given DEK using the KEK registered for the specified namespace.
// It returns DEKMaterial containing the KEK ID and the base64-encoded ciphertext.
func (r *KEKRegistry) Encrypt(ctx context.Context, ns string, dek *DEK) (*DEKMaterial, error) {
	k, ok := r.keks[ns]
	if !ok {
		k = r.defaultKey
	}

	ct, err := k.Encrypt(ctx, dek.key)
	if err != nil {
		return nil, fmt.Errorf("failed to encrypt message in namespace: %s, %w", ns, err)
	}

	return &DEKMaterial{
		KEKID:        k.ID(),
		EncryptedDEK: base64.StdEncoding.EncodeToString(ct),
	}, nil
}

// Decrypt decrypts the DEK described by m using the KEK identified by m.KEKID.
func (r *KEKRegistry) Decrypt(ctx context.Context, m *DEKMaterial) (*DEK, error) {
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
		errs := make([]error, 0, len(r.keks))
		for k, kek := range r.keks {
			if err := kek.Close(); err != nil {
				errs = append(errs, fmt.Errorf("failed to close KEK: %s, %w", k, err))
			}
		}
		r.closeErr = errors.Join(errs...)
	})

	return r.closeErr
}

func (f kekRegOpt) apply(r *KEKRegistry) {
	f(r)
}

func (k *nilKEK) ID() string                                           { return "EMPTY_KEK" }
func (k *nilKEK) Encrypt(_ context.Context, pt []byte) ([]byte, error) { return pt, nil }
func (k *nilKEK) Decrypt(_ context.Context, ct []byte) ([]byte, error) { return ct, nil }
func (k *nilKEK) Close() error                                         { return nil }
