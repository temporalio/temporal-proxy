package crypto

import (
	"context"
	"fmt"
	"sync"
	"time"
)

type (
	// Sealer manages per-ID DEK rotation and envelope encryption.
	//
	// Each ID (typically a Temporal namespace) has its own [DEK] with an
	// independent rotation schedule defined by a [KeyConfig]. DEKs are
	// pre-rotated by a background [Sealer.Refresh] call before their
	// expiration buffer is reached, keeping key generation off the hot path.
	//
	// Use [Sealer.Seal] to encrypt data and [Sealer.Open] to decrypt it.
	Sealer struct {
		mu       sync.RWMutex
		keys     map[string]*sealerDEK
		registry *KEKRegistry
		now      func() time.Time
	}

	sealerOptions struct {
		config map[string]*KeyConfig
		now    func() time.Time
	}

	// SealerOption configures a [Sealer] during construction.
	SealerOption func(*sealerOptions)

	// Envelope is the output of a [Sealer.Seal] call. It carries the
	// AES-256-GCM ciphertext together with the [DEKMaterial] required to
	// recover the DEK for decryption via [Sealer.Open].
	Envelope struct {
		CipherText  []byte
		KeyMaterial *DEKMaterial
	}

	// KeyConfig defines the config (e.g. rotation policy) for a single key slot.
	KeyConfig struct {
		Lifetime         time.Duration // How long a DEK remains valid
		ExpirationBuffer time.Duration // How far before expiry [Sealer.Refresh] should pre-rotate the key.
	}

	sealerDEK struct {
		key    *DEK             // The current DEK
		exp    time.Time        // When this key expires
		incr   time.Duration    // How long each DEK is valid for
		expBuf time.Duration    // How long before expiry should we regenerate
		now    func() time.Time // Clock source, shared with the parent Sealer
	}
)

// NewSealer constructs a Sealer backed by r, applying opts in order.
// It generates an initial [DEK] for every ID registered via [WithKeyConfig].
// Returns an error if any DEK generation fails.
func NewSealer(r *KEKRegistry, opts ...SealerOption) (*Sealer, error) {
	sopts := &sealerOptions{
		config: make(map[string]*KeyConfig),
		now:    time.Now,
	}

	for _, opt := range opts {
		opt(sopts)
	}

	s := &Sealer{
		keys:     make(map[string]*sealerDEK),
		registry: r,
		now:      sopts.now,
	}

	for k, v := range sopts.config {
		key, err := NewDEK()
		if err != nil {
			return nil, err
		}

		s.keys[k] = &sealerDEK{
			incr:   v.Lifetime,
			expBuf: v.ExpirationBuffer,
			now:    s.now,
		}

		// NB: No locks needed in constructor
		s.keys[k].rotate(key)
	}

	return s, nil
}

// WithKeyConfig registers a [KeyConfig] rotation policy for id.
func WithKeyConfig(id string, cfg KeyConfig) SealerOption {
	return func(s *sealerOptions) {
		if _, ok := s.config[id]; ok {
			panic(fmt.Sprintf("Duplicate key config for '%s'", id))
		}

		s.config[id] = &cfg
	}
}

// WithNowFunc overrides the clock used for DEK expiry checks and rotation
// timestamps. Intended for testing; production code should use the default
// [time.Now].
func WithNowFunc(fn func() time.Time) SealerOption {
	return func(s *sealerOptions) {
		s.now = fn
	}
}

// Seal encrypts data using the DEK for id, returning an [Envelope] that
// contains the ciphertext and the [DEKMaterial] needed to decrypt it later.
func (s *Sealer) Seal(ctx context.Context, id string, data []byte) (*Envelope, error) {
	dek, err := s.getOrRefreshKey(id)
	if err != nil {
		return nil, err
	}

	ct, err := dek.Encrypt(ctx, data)
	if err != nil {
		return nil, err
	}

	m, err := s.registry.Encrypt(ctx, id, dek)
	if err != nil {
		return nil, err
	}

	return &Envelope{
		CipherText:  ct,
		KeyMaterial: m,
	}, nil
}

// Open decrypts e by recovering the DEK from the registry and returning the
// original plaintext.
func (s *Sealer) Open(ctx context.Context, e *Envelope) ([]byte, error) {
	dek, err := s.registry.Decrypt(ctx, e.KeyMaterial)
	if err != nil {
		return nil, err
	}

	pt, err := dek.Decrypt(ctx, e.CipherText)
	if err != nil {
		return nil, err
	}

	return pt, nil
}

// Refresh rotates every expired DEK. It is intended to be called periodically
// from a background goroutine so that key rotation stays off the [Sealer.Seal]
// hot path.
func (s *Sealer) Refresh() error {
	// Find expired keys without acquiring a write lock.
	s.mu.RLock()
	expKeys := make([]string, 0, len(s.keys))
	for k, v := range s.keys {
		if v.isExpired() {
			expKeys = append(expKeys, k)
		}
	}
	s.mu.RUnlock()

	// Generate DEKs without the lock as well.
	updates := make(map[string]*DEK, len(expKeys))
	for _, k := range expKeys {
		dek, err := NewDEK()
		if err != nil {
			return fmt.Errorf("failed to create DEK for %s: %w", k, err)
		}

		updates[k] = dek
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	for k, d := range updates {
		s.keys[k].rotate(d)
	}

	return nil
}

func (s *Sealer) getOrRefreshKey(id string) (*DEK, error) {
	s.mu.RLock()
	key := s.keys[id] // keys were created in the constructor.
	if key == nil {
		s.mu.RUnlock()
		return nil, fmt.Errorf("key not found, id: %s", id)
	}

	if !key.isExpired() {
		dek := key.key // snapshot under RLock — caller never touches expiringDEK directly
		s.mu.RUnlock()
		return dek, nil
	}
	s.mu.RUnlock()

	// Ideally we wouldn't get here. This is the case when Refresh hasn't been called, but the key is expired. This can
	// happen when the ExpirationBuffer < the Refresh interval (e.g. Refresh every minute, but ExpirationBuffer is 30s),
	// or with just bad luck/timing.
	//
	// The goal remains to refresh keys optimistically before they expire, so we don't generate keys in the hot path
	// (when encrypting data).
	s.mu.Lock()
	defer s.mu.Unlock()

	// Re-check after acquiring the write lock: Refresh (or another Seal) may
	// have already rotated this key in the window between RUnlock and Lock.
	if !key.isExpired() {
		return key.key, nil
	}

	k, err := NewDEK()
	if err != nil {
		return nil, fmt.Errorf("failed to generate DEK for %s: %w", id, err)
	}

	key.rotate(k)
	return key.key, nil
}

func (d *sealerDEK) isExpired() bool {
	return d.exp.Add(-d.expBuf).Before(d.now())
}

func (d *sealerDEK) rotate(newKey *DEK) {
	d.key = newKey
	d.exp = d.now().Add(d.incr)
}
