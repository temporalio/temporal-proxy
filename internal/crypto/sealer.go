package crypto

import (
	"context"
	"fmt"
	"sync"
	"time"

	lru "github.com/hashicorp/golang-lru/v2"
	"golang.org/x/sync/singleflight"
)

const defaultCacheSize = 100

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
		mu        sync.RWMutex
		keys      map[string]*sealerDEK
		registry  *KEKRegistry
		now       func() time.Time
		dekCache  *lru.Cache[string, *DEK] // keyed by EncryptedDEK; nil if cacheSize == 0
		encryptor singleflight.Group       // coalesces concurrent registry.Encrypt calls per namespace
	}

	sealerOptions struct {
		config    map[string]*KeyConfig
		now       func() time.Time
		cacheSize int // <=0 = no cache
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
		key      *DEK             // The current DEK
		material *DEKMaterial     // KMS-wrapped DEK; nil after rotation, protected by Sealer.mu
		exp      time.Time        // When this key expires
		incr     time.Duration    // How long each DEK is valid for
		expBuf   time.Duration    // How long before expiry should we regenerate
		now      func() time.Time // Clock source, shared with the parent Sealer
	}
)

// NewSealer constructs a Sealer backed by r, applying opts in order.
// It generates an initial [DEK] for every ID registered via [WithKeyConfig].
// Returns an error if any DEK generation fails.
func NewSealer(r *KEKRegistry, opts ...SealerOption) (*Sealer, error) {
	sopts := &sealerOptions{
		cacheSize: defaultCacheSize,
		config:    make(map[string]*KeyConfig),
		now:       time.Now,
	}

	for _, opt := range opts {
		opt(sopts)
	}

	s := &Sealer{
		keys:     make(map[string]*sealerDEK),
		registry: r,
		now:      sopts.now,
	}

	if sopts.cacheSize > 0 {
		c, err := lru.New[string, *DEK](sopts.cacheSize)
		if err != nil {
			return nil, fmt.Errorf("failed to create DEK cache: %w", err)
		}
		s.dekCache = c
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

// WithCacheSize sets the maximum number of decrypted DEKs the Open cache may
// hold. Values ≤ 0 disable the cache, overriding the default. Each entry is
// keyed by the base64-encoded encrypted DEK string.
func WithCacheSize(n int) SealerOption {
	return func(s *sealerOptions) {
		s.cacheSize = n
	}
}

// Seal encrypts data using the DEK for id, returning an [Envelope] that
// contains the ciphertext and the [DEKMaterial] needed to decrypt it later.
func (s *Sealer) Seal(ctx context.Context, id string, data []byte) (*Envelope, error) {
	sd, err := s.getOrRefreshKey(id)
	if err != nil {
		return nil, err
	}

	// Snapshot key and material under RLock; release immediately so the
	// encryption work happens outside the lock.
	s.mu.RLock()
	dek := sd.key
	material := sd.material
	s.mu.RUnlock()

	ct, err := dek.Encrypt(ctx, data)
	if err != nil {
		return nil, err
	}

	if material != nil {
		return &Envelope{CipherText: ct, KeyMaterial: material}, nil
	}

	// First Seal after a rotation: coalesce concurrent callers for the same namespace
	// into a single KMS call so bursts don't trigger rate limiting in the Cloud provider.
	//
	// Key on both the namespace and the DEK pointer. If the KMS call is slow and
	// Refresh rotates the DEK while it is in-flight, a goroutine that snapshotted
	// the new DEK must not join the old call as it would receive material for DEK1
	// while its ciphertext was encrypted with DEK2, causing Open to fail. Callers
	// holding the same DEK pointer still coalesce as intended.
	sfKey := fmt.Sprintf("%s:%p", id, dek)
	v, err, _ := s.encryptor.Do(sfKey, func() (any, error) {
		return s.registry.Encrypt(ctx, id, dek)
	})
	if err != nil {
		return nil, err
	}
	m := v.(*DEKMaterial)

	// Store under write lock.
	// Only touch sd.material when sd.key == dek: if the key was rotated between
	// our snapshot and now we must not read or write material that belongs to a
	// different DEK. The envelope we built (ct + m) is still correct for this call.
	// When sd.key == dek, prefer an already-stored value so all concurrent callers
	// for the same DEK return identical material.
	s.mu.Lock()
	if sd.key == dek {
		if sd.material == nil {
			sd.material = m
		} else {
			m = sd.material
		}
	}
	s.mu.Unlock()

	return &Envelope{CipherText: ct, KeyMaterial: m}, nil
}

// Open decrypts e by recovering the DEK from the registry and returning the
// original plaintext.
func (s *Sealer) Open(ctx context.Context, e *Envelope) ([]byte, error) {
	var dek *DEK
	if s.dekCache != nil {
		if cached, ok := s.dekCache.Get(e.KeyMaterial.EncryptedDEK); ok {
			dek = cached
		}
	}

	if dek == nil {
		var err error
		dek, err = s.registry.Decrypt(ctx, e.KeyMaterial)
		if err != nil {
			return nil, err
		}
		if s.dekCache != nil {
			s.dekCache.Add(e.KeyMaterial.EncryptedDEK, dek)
		}
	}

	return dek.Decrypt(ctx, e.CipherText)
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

func (s *Sealer) getOrRefreshKey(id string) (*sealerDEK, error) {
	s.mu.RLock()
	key := s.keys[id]
	if key == nil {
		s.mu.RUnlock()
		return nil, fmt.Errorf("key not found, id: %s", id)
	}

	if !key.isExpired() {
		s.mu.RUnlock()
		return key, nil
	}
	s.mu.RUnlock()

	// Ideally we wouldn't get here. This is the case when Refresh hasn't been called, but the key is expired.
	s.mu.Lock()
	defer s.mu.Unlock()

	// Re-check: Refresh (or another Seal) may have already rotated this key.
	if !key.isExpired() {
		return key, nil
	}

	k, err := NewDEK()
	if err != nil {
		return nil, fmt.Errorf("failed to generate DEK for %s: %w", id, err)
	}

	key.rotate(k)
	return key, nil
}

func (d *sealerDEK) isExpired() bool {
	return d.exp.Add(-d.expBuf).Before(d.now())
}

func (d *sealerDEK) rotate(newKey *DEK) {
	d.key = newKey
	d.material = nil
	d.exp = d.now().Add(d.incr)
}
