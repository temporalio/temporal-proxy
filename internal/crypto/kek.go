package crypto

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"maps"
	"sync"
	"time"
)

// ErrNamespaceAlreadyExists is returned when attempting to set a [KeyPolicy] for a namespace that
// already has a definition.
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

	// KeyPolicy bundles a KEK with the DEK lifecycle parameters for a namespace.
	// Registering a namespace with [KEKRegistry] requires a KeyPolicy, ensuring the
	// cryptographic key and its rotation schedule are always configured together.
	KeyPolicy struct {
		KEK         KEK
		Duration    time.Duration // How long a DEK remains valid.
		RenewBefore time.Duration // How far before expiry [Sealer.Refresh] should pre-rotate the DEK.
	}

	// KEKRegistry holds the set of KEKs available for encrypting and decrypting DEKs.
	// It is keyed by namespace (for encryption) and by key ID (for decryption).
	// Close must be called when the registry is no longer needed to release KEK resources.
	KEKRegistry struct {
		defaultPolicy KeyPolicy            // Fallback when no namespace policy is registered.
		keks          map[string]KeyPolicy // map from NS -> KeyPolicy
		keyIDs        map[string]KEK       // map from ID -> KEK

		closeOnce sync.Once
		closeErr  error
	}

	// KEKRegistryOption configures a KEKRegistry during construction.
	KEKRegistryOption func(*KEKRegistry)

	// nilKEK defines a KEK that does nothing.
	nilKEK struct{}
)

// NewKEKRegistry constructs a KEKRegistry, applying opts in order.
// The key-ID index used by Decrypt is built after all options are applied.
func NewKEKRegistry(opts ...KEKRegistryOption) *KEKRegistry {
	r := &KEKRegistry{
		keks: map[string]KeyPolicy{},
	}
	for _, opt := range opts {
		opt(r)
	}

	// Always seed nilKEK so data encrypted without a real KEK can still be decrypted.
	nk := new(nilKEK)
	idMap := make(map[string]KEK, len(r.keks)+1)
	idMap[nk.ID()] = nk

	if r.defaultPolicy.KEK != nil {
		idMap[r.defaultPolicy.KEK.ID()] = r.defaultPolicy.KEK
	}
	for _, p := range r.keks {
		idMap[p.KEK.ID()] = p.KEK
	}

	r.keyIDs = idMap
	return r
}

// WithDefaultPolicy sets the fallback [KeyPolicy] used when no namespace-specific policy is registered.
func WithDefaultPolicy(p KeyPolicy) KEKRegistryOption {
	return func(r *KEKRegistry) {
		r.defaultPolicy = p
	}
}

// WithKeyPolicy registers p for ns, used when encrypting or decrypting DEKs for that namespace.
func WithKeyPolicy(ns string, p KeyPolicy) KEKRegistryOption {
	return func(r *KEKRegistry) {
		if _, ok := r.keks[ns]; ok {
			panic(fmt.Sprintf("Key for namespace '%s' already defined", ns))
		}

		r.keks[ns] = p
	}
}

// Policies returns a copy of all per-namespace [KeyPolicy] entries.
func (r *KEKRegistry) Policies() map[string]KeyPolicy {
	out := make(map[string]KeyPolicy, len(r.keks))
	maps.Copy(out, r.keks)

	return out
}

// DefaultPolicy returns the fallback [KeyPolicy].
func (r *KEKRegistry) DefaultPolicy() KeyPolicy {
	return r.defaultPolicy
}

// Encrypt encrypts the given DEK using the KEK registered for the specified namespace.
// It returns DEKMaterial containing the KEK ID and the base64-encoded ciphertext.
func (r *KEKRegistry) Encrypt(ctx context.Context, ns string, dek *DEK) (*DEKMaterial, error) {
	policy, ok := r.keks[ns]
	if !ok {
		policy = r.defaultPolicy
	}

	kek := policy.KEK
	if kek == nil {
		kek = new(nilKEK)
	}

	ct, err := kek.Encrypt(ctx, dek.key)
	if err != nil {
		return nil, fmt.Errorf("failed to encrypt message in namespace: %s, %w", ns, err)
	}

	return &DEKMaterial{
		KEKID:        kek.ID(),
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
	// NB: Blocking concurrent callers here is acceptable: Close is a shutdown
	// operation; callers should not race to close, and if they do, waiting
	// for a single authoritative result is the right behaviour.
	r.closeOnce.Do(func() {
		errs := make([]error, 0, len(r.keks)+1)
		if r.defaultPolicy.KEK != nil {
			if err := r.defaultPolicy.KEK.Close(); err != nil {
				errs = append(errs, fmt.Errorf("failed to close default KEK: %w", err))
			}
		}

		for k, policy := range r.keks {
			if err := policy.KEK.Close(); err != nil {
				errs = append(errs, fmt.Errorf("failed to close KEK: %s, %w", k, err))
			}
		}

		r.closeErr = errors.Join(errs...)
	})

	return r.closeErr
}

func (k *nilKEK) ID() string                                           { return "EMPTY_KEK" }
func (k *nilKEK) Encrypt(_ context.Context, pt []byte) ([]byte, error) { return pt, nil }
func (k *nilKEK) Decrypt(_ context.Context, ct []byte) ([]byte, error) { return ct, nil }
func (k *nilKEK) Close() error                                         { return nil }
