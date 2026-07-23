package crypto

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	lru "github.com/hashicorp/golang-lru/v2"
	"golang.org/x/sync/singleflight"
)

type (
	// Vault provides envelope encryption scoped by namespace. It keeps a sliding
	// Data Encryption Key ([DEK]) per namespace, wrapping each DEK with the KEK
	// selected for that namespace by a [KEKRegistry]. DEKs are rotated on a
	// sliding schedule (see [KeyConfig]) and decrypted DEKs are cached to avoid
	// repeated KMS calls on Open. A Vault is safe for concurrent use.
	Vault struct {
		mu            sync.RWMutex
		keys          map[string]*slidingDEK
		registry      *KEKRegistry
		encryptor     singleflight.Group
		cache         *lru.Cache[string, *DEK]
		defaultConfig *KeyConfig
		nowFn         func() time.Time
	}

	// NamespacedVault is a [Vault] bound to a single namespace so callers can
	// Seal and Open without passing the namespace on every call. Obtain one via
	// [Vault.ForNamespace].
	NamespacedVault struct {
		inner *Vault
		ns    string
	}

	// KeyConfig controls the lifetime of a namespace's DEK.
	KeyConfig struct {
		// Duration is how long a DEK is valid before it must be rotated.
		Duration time.Duration
		// RenewBefore causes a DEK to be treated as expired this long before
		// Duration elapses, so it can be rotated ahead of its actual expiry.
		RenewBefore time.Duration
	}

	// Message is the result of sealing plaintext: the AES-256-GCM ciphertext
	// together with the wrapped DEK ([DEKMaterial]) required to open it.
	Message struct {
		Ciphertext  []byte
		KeyMaterial *DEKMaterial
	}

	// VaultOption configures a [Vault] during construction.
	VaultOption func(*vaultOptions)

	vaultOptions struct {
		config        map[string]KeyConfig
		defaultConfig *KeyConfig
		nowFn         func() time.Time
		cacheSize     int
		errs          []error
	}

	slidingDEK struct {
		key      *DEK
		material *DEKMaterial
		exp      time.Time
		dur      time.Duration
		before   time.Duration
	}
)

// NewVault constructs a Vault backed by r, applying opts in order. A DEK is
// pre-generated for every namespace registered via [WithKeyConfig]. NewVault
// returns an error if any option is invalid (for example, a duplicate namespace
// config) or if key/cache setup fails.
func NewVault(r *KEKRegistry, opts ...VaultOption) (*Vault, error) {
	if r == nil {
		return nil, errors.New("registry must not be nil")
	}

	vopts := &vaultOptions{
		cacheSize: 100,
		config:    make(map[string]KeyConfig),
		nowFn:     time.Now,
	}

	for _, opt := range opts {
		opt(vopts)
	}

	if len(vopts.errs) > 0 {
		return nil, errors.Join(vopts.errs...)
	}

	vault := &Vault{
		registry:      r,
		keys:          make(map[string]*slidingDEK),
		defaultConfig: vopts.defaultConfig,
		nowFn:         vopts.nowFn,
	}

	if vopts.cacheSize > 0 {
		c, err := lru.New[string, *DEK](vopts.cacheSize)
		if err != nil {
			return nil, fmt.Errorf("failed to create DEK cache: %w", err)
		}

		vault.cache = c
	}

	for k, v := range vopts.config {
		key, err := NewDEK()
		if err != nil {
			return nil, err
		}

		vault.keys[k] = &slidingDEK{
			key:    key,
			dur:    v.Duration,
			before: v.RenewBefore,
		}

		// NB: No need for locks while initializing.
		vault.keys[k].rotate(key, vault.nowFn)
	}

	return vault, nil
}

// WithDefaultKeyConfig sets the KeyConfig used for namespaces that have no
// explicit [WithKeyConfig]. Without a default, sealing an unconfigured
// namespace fails; with one, a DEK is created for such namespaces on first use.
func WithDefaultKeyConfig(cfg KeyConfig) VaultOption {
	return func(e *vaultOptions) {
		if err := cfg.validate(); err != nil {
			e.errs = append(e.errs, fmt.Errorf("invalid default key config: %w", err))
			return
		}

		e.defaultConfig = &cfg
	}
}

// WithKeyConfig sets the KeyConfig for a specific namespace. Registering the
// same namespace more than once is an error surfaced by [NewVault].
func WithKeyConfig(ns string, cfg KeyConfig) VaultOption {
	return func(e *vaultOptions) {
		if _, ok := e.config[ns]; ok {
			e.errs = append(e.errs, fmt.Errorf("duplicate key config for %q", ns))
			return
		}
		if err := cfg.validate(); err != nil {
			e.errs = append(e.errs, fmt.Errorf("key config for %q: %w", ns, err))
			return
		}

		e.config[ns] = cfg
	}
}

// WithNowFunc overrides the clock used to evaluate DEK expiry. It is primarily
// useful in tests. A nil function is rejected by [NewVault].
func WithNowFunc(fn func() time.Time) VaultOption {
	return func(e *vaultOptions) {
		if fn == nil {
			e.errs = append(e.errs, errors.New("now func must not be nil"))
			return
		}

		e.nowFn = fn
	}
}

// WithCacheSize sets the maximum number of decrypted DEKs retained in the Open
// cache. A value of zero or less disables the cache, so every Open unwraps its
// DEK via the [KEKRegistry].
func WithCacheSize(n int) VaultOption {
	return func(e *vaultOptions) {
		e.cacheSize = max(0, n)
	}
}

// Seal encrypts data for ns, returning the ciphertext together with the wrapped
// DEK required to Open it. The active DEK for ns is created or rotated on demand.
// Concurrent first-time seals holding the same DEK are coalesced into a single
// KEK (KMS) call.
func (v *Vault) Seal(ctx context.Context, ns string, data []byte) (*Message, error) {
	sd, err := v.getOrRefreshKey(ns)
	if err != nil {
		return nil, err
	}

	// Snapshot key and material under RLock; release immediately so the
	// encryption work happens outside the lock.
	v.mu.RLock()
	dek := sd.key
	material := sd.material
	v.mu.RUnlock()

	ct, err := dek.Encrypt(ctx, data)
	if err != nil {
		return nil, err
	}

	if material != nil {
		return &Message{Ciphertext: ct, KeyMaterial: material}, nil
	}

	// First Seal after a rotation: coalesce concurrent callers for the same namespace
	// into a single KMS call so bursts don't trigger rate limiting in the Cloud provider.
	//
	// Key on both the namespace and the DEK pointer. If the KMS call is slow and
	// Refresh rotates the DEK while it is in-flight, a goroutine that snapshotted
	// the new DEK must not join the old call as it would receive material for DEK1
	// while its ciphertext was encrypted with DEK2, causing Open to fail. Callers
	// holding the same DEK pointer still coalesce as intended.
	sfKey := fmt.Sprintf("%s:%p", ns, dek)
	res, err, _ := v.encryptor.Do(sfKey, func() (any, error) {
		return v.registry.Encrypt(ctx, ns, dek)
	})
	if err != nil {
		return nil, err
	}
	m := res.(*DEKMaterial)

	// Store under write lock.
	// Only touch sd.material when sd.key == dek: if the key was rotated between
	// our snapshot and now we must not read or write material that belongs to a
	// different DEK. The envelope we built (ct + m) is still correct for this call.
	// When sd.key == dek, prefer an already-stored value so all concurrent callers
	// for the same DEK return identical material.
	v.mu.Lock()
	if sd.key == dek {
		if sd.material == nil {
			sd.material = m
		} else {
			m = sd.material
		}
	}
	v.mu.Unlock()

	return &Message{Ciphertext: ct, KeyMaterial: m}, nil
}

// Open decrypts msg, which must have been produced by [Vault.Seal] (or
// [NamespacedVault.Seal]). The wrapped DEK is unwrapped via the [KEKRegistry]
// using the KEK identified by the material carried in msg, served from the
// decrypted-DEK cache when it is enabled.
func (v *Vault) Open(ctx context.Context, msg *Message) ([]byte, error) {
	if msg == nil || msg.KeyMaterial == nil {
		return nil, errors.New("message and key material must not be nil")
	}

	var dek *DEK
	if v.cache != nil {
		if cached, ok := v.cache.Get(msg.KeyMaterial.EncryptedDEK); ok {
			dek = cached
		}
	}

	if dek == nil {
		var err error
		dek, err = v.registry.Decrypt(ctx, msg.KeyMaterial)
		if err != nil {
			return nil, err
		}

		if v.cache != nil {
			v.cache.Add(msg.KeyMaterial.EncryptedDEK, dek)
		}
	}

	return dek.Decrypt(ctx, msg.Ciphertext)
}

// Refresh rotates every namespace DEK that has reached its renewal threshold.
// It is meant to be called periodically. Seal also rotates an expired DEK on
// demand, so Refresh is an optimization that keeps rotation off the request
// path rather than a correctness requirement.
func (v *Vault) Refresh() error {
	// Find expired keys without acquiring a write lock.
	v.mu.RLock()
	expKeys := make([]string, 0, len(v.keys))
	for ns, dek := range v.keys {
		if dek.isExpired(v.nowFn) {
			expKeys = append(expKeys, ns)
		}
	}
	v.mu.RUnlock()

	// Generate DEKs without the lock as well.
	updates := make(map[string]*DEK, len(expKeys))
	for _, k := range expKeys {
		dek, err := NewDEK()
		if err != nil {
			return fmt.Errorf("failed to create DEK for %s: %w", k, err)
		}

		updates[k] = dek
	}

	v.mu.Lock()
	defer v.mu.Unlock()
	for k, d := range updates {
		// Re-check: a concurrent Seal (or Refresh) may have rotated this key
		// between the scan above and this write lock. Rotating again would
		// discard that fresh DEK and force an extra KEK wrap on the next Seal.
		if v.keys[k].isExpired(v.nowFn) {
			v.keys[k].rotate(d, v.nowFn)
		}
	}

	return nil
}

// ForNamespace returns a [NamespacedVault] that seals and opens within ns.
func (v *Vault) ForNamespace(ns string) *NamespacedVault {
	return &NamespacedVault{
		inner: v,
		ns:    ns,
	}
}

func (v *Vault) getOrRefreshKey(ns string) (*slidingDEK, error) {
	v.mu.RLock()
	key := v.keys[ns]
	if key == nil {
		v.mu.RUnlock()
		if v.defaultConfig == nil {
			return nil, fmt.Errorf("key not found, ns: %s", ns)
		}

		return v.createDefaultKey(ns)
	}

	if !key.isExpired(v.nowFn) {
		v.mu.RUnlock()
		return key, nil
	}
	v.mu.RUnlock()

	// Ideally we wouldn't get here. This is the case when Refresh hasn't been called, but the key is expired.
	v.mu.Lock()
	defer v.mu.Unlock()

	// Re-check: Refresh (or another Seal) may have already rotated this key.
	if !key.isExpired(v.nowFn) {
		return key, nil
	}

	k, err := NewDEK()
	if err != nil {
		return nil, fmt.Errorf("failed to generate DEK for %s: %w", ns, err)
	}

	key.rotate(k, v.nowFn)
	return key, nil
}

func (v *Vault) createDefaultKey(ns string) (*slidingDEK, error) {
	v.mu.Lock()
	defer v.mu.Unlock()

	// Re-check: another goroutine may have created this slot while we waited for the lock.
	if key := v.keys[ns]; key != nil {
		return key, nil
	}

	dek, err := NewDEK()
	if err != nil {
		return nil, fmt.Errorf("failed to generate DEK for %s: %w", ns, err)
	}

	sd := &slidingDEK{
		dur:    v.defaultConfig.Duration,
		before: v.defaultConfig.RenewBefore,
	}

	sd.rotate(dek, v.nowFn)
	v.keys[ns] = sd

	return sd, nil
}

// Seal encrypts data within the bound namespace. See [Vault.Seal].
func (v *NamespacedVault) Seal(ctx context.Context, data []byte) (*Message, error) {
	return v.inner.Seal(ctx, v.ns, data)
}

// Open decrypts msg within the bound namespace. See [Vault.Open].
func (v *NamespacedVault) Open(ctx context.Context, msg *Message) ([]byte, error) {
	return v.inner.Open(ctx, msg)
}

func (c KeyConfig) validate() error {
	if c.Duration <= 0 {
		return fmt.Errorf("duration must be positive, got %s", c.Duration)
	}

	if c.RenewBefore < 0 || c.RenewBefore >= c.Duration {
		return fmt.Errorf("renew before must be in [0, %s), got %s", c.Duration, c.RenewBefore)
	}

	return nil
}

func (d *slidingDEK) isExpired(now func() time.Time) bool {
	return d.exp.Add(-d.before).Before(now())
}

func (d *slidingDEK) rotate(newKey *DEK, now func() time.Time) {
	d.key = newKey
	d.material = nil
	d.exp = now().Add(d.dur)
}
