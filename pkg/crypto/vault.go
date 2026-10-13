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
		observer      Observer
		binding       BindingMode
	}

	// NamespacedVault is a [Vault] bound to a single namespace so callers can
	// Seal and Open without passing the namespace on every call. Obtain one via
	// [Vault.ForNamespace].
	NamespacedVault struct {
		inner *Vault
		ns    string
	}

	// KeyConfig controls the lifetime of a namespace's DEK. [NewVault] rejects
	// a config unless Duration > 0 and 0 <= RenewBefore < Duration.
	KeyConfig struct {
		// Duration is how long a DEK is valid before it must be rotated.
		Duration time.Duration
		// RenewBefore causes a DEK to be treated as expired this long before
		// Duration elapses, so it can be rotated ahead of its actual expiry.
		RenewBefore time.Duration
	}

	// BindingMode controls whether [Vault.Seal] binds a payload to its namespace
	// and whether [Vault.Open] accepts one that is not bound. A bound payload's
	// namespace is authenticated as AES-GCM additional data, so a message whose
	// binding was altered, stripped, or added fails to open. Every mode opens
	// bound messages, which is what lets a fleet upgrade before any replica
	// starts binding. Set it with [WithBindingMode].
	BindingMode int

	// Message is the result of sealing plaintext: the AES-256-GCM ciphertext
	// together with the wrapped DEK ([DEKMaterial]) required to open it. Bound
	// reports whether the ciphertext is bound to Namespace through its additional
	// data; the zero value is an unbound (legacy) message, and Namespace is empty
	// unless Bound. A bound message may be bound to the empty namespace, which is
	// distinct from being unbound.
	Message struct {
		Ciphertext  []byte
		KeyMaterial *DEKMaterial
		Bound       bool
		Namespace   string
	}

	// VaultOption configures a [Vault] during construction.
	VaultOption func(*vaultOptions)

	// SealOption configures a single [Vault.Seal].
	SealOption func(*sealOptions)

	// OpenOption configures a single [Vault.Open].
	OpenOption func(*openOptions)

	sealOptions struct {
		label string
	}

	openOptions struct {
		expect    string
		hasExpect bool
	}

	vaultOptions struct {
		config        map[string]KeyConfig
		defaultConfig *KeyConfig
		nowFn         func() time.Time
		cacheSize     int
		observer      Observer
		binding       BindingMode
		errs          []error
	}

	slidingDEK struct {
		key      *DEK
		material *DEKMaterial
		exp      time.Time
		dur      time.Duration
		before   time.Duration
	}

	// cryptoStep records whether the AES-256-GCM step ran, what it cost, and
	// whether it failed. Its zero value means the step was never reached, which
	// is what an early failure returns. ran is tracked explicitly because a step
	// that did run can measure a zero dur within the clock's resolution.
	cryptoStep struct {
		ran bool
		dur time.Duration
		err error
	}
)

// DefaultCacheSize is the number of decrypted DEKs a Vault retains when no
// [WithCacheSize] option is given. It is exported so that a caller which
// defaults the option for itself - configuration, say - inherits this number
// rather than restating it and drifting from it.
const DefaultCacheSize = 100

// The binding modes, in rollout order. See [BindingMode].
const (
	// BindingOff seals unbound messages and opens both bound and unbound ones.
	// It is the zero value, so a Vault built without [WithBindingMode] changes
	// nothing on the wire.
	BindingOff BindingMode = iota
	// BindingPermissive seals bound messages and opens both bound and unbound
	// ones.
	BindingPermissive
	// BindingStrict seals bound messages and rejects unbound ones with
	// [ErrUnbound].
	BindingStrict
)

var (
	// ErrUnbound is returned by [Vault.Open] under [BindingStrict] for a message
	// that is not bound to a namespace and whose KEK the Vault holds. One whose
	// KEK it does not hold returns [ErrUnknownKey] instead, so a chained proxy can
	// still pass it through.
	ErrUnbound = errors.New("message is not bound to a namespace")

	// ErrNamespaceMismatch is returned by [Vault.Open] under [BindingStrict] for
	// a bound message whose namespace differs from the one given through
	// [ExpectNamespace], when the Vault holds its KEK. As with [ErrUnbound], one
	// whose KEK it does not hold returns [ErrUnknownKey] instead.
	ErrNamespaceMismatch = errors.New("message is bound to a different namespace")
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
		cacheSize: DefaultCacheSize,
		config:    make(map[string]KeyConfig),
		nowFn:     time.Now,
		observer:  nopObserver{},
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
		observer:      vopts.observer,
		binding:       vopts.binding,
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
// same namespace more than once, or passing a cfg that breaks the [KeyConfig]
// constraints, is an error surfaced by [NewVault].
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

// WithObserver sets the Observer notified of Vault-internal events: a
// CacheEvent from Open, an EnvelopeEvent from both Seal and Open, and a
// RotationEvent from Seal and Refresh. A nil Observer is replaced with a
// no-op, so no Vault call site needs to nil-check. Without this option no
// events are emitted.
func WithObserver(o Observer) VaultOption {
	return func(e *vaultOptions) {
		if o == nil {
			o = nopObserver{}
		}

		e.observer = o
	}
}

// WithBindingMode sets how the Vault binds payloads to their namespace. Without
// it the mode is [BindingOff]. A value other than the declared modes is
// rejected by [NewVault].
func WithBindingMode(m BindingMode) VaultOption {
	return func(e *vaultOptions) {
		if m < BindingOff || m > BindingStrict {
			e.errs = append(e.errs, fmt.Errorf("unknown binding mode %d", m))
			return
		}

		e.binding = m
	}
}

// WithLabel sets the namespace a sealed message is bound to, which otherwise is
// the namespace that selects the key. It lets a caller key by one name and bind
// by another, such as a proxy that keys by local name and binds by the name the
// upstream knows. It has no effect under [BindingOff].
func WithLabel(label string) SealOption {
	return func(o *sealOptions) { o.label = label }
}

// ExpectNamespace sets the namespace a bound message must be bound to. Under
// [BindingStrict] a bound message bound to any other namespace fails with
// [ErrNamespaceMismatch]; other modes only open it. Without this option Open
// compares nothing.
func ExpectNamespace(ns string) OpenOption {
	return func(o *openOptions) {
		o.expect = ns
		o.hasExpect = true
	}
}

// Seal encrypts data for ns, returning the ciphertext together with the wrapped
// DEK required to Open it. The active DEK for ns is created or rotated on demand.
// Concurrent first-time seals holding the same DEK are coalesced into a single
// KEK (KMS) call. Unless the mode is [BindingOff], the message is bound to ns,
// or to the label given through [WithLabel].
//
// Exactly one [EnvelopeEvent] is reported to the Observer, on every path
// including failures.
func (v *Vault) Seal(ctx context.Context, ns string, data []byte, opts ...SealOption) (*Message, error) {
	so := sealOptions{label: ns}
	for _, opt := range opts {
		opt(&so)
	}

	start := time.Now()
	msg, step, err := v.seal(ctx, ns, so.label, data)

	v.observer.Observe(EnvelopeEvent{
		Op:              OpEncrypt,
		Namespace:       ns,
		Err:             err,
		CryptoAttempted: step.ran,
		CryptoErr:       step.err,
		Total:           time.Since(start),
		Crypto:          step.dur,
	})

	return msg, err
}

// Open decrypts msg, which must have been produced by [Vault.Seal] (or
// [NamespacedVault.Seal]). The wrapped DEK is unwrapped via the [KEKRegistry]
// using the KEK identified by the material carried in msg, served from the
// decrypted-DEK cache when it is enabled. Under [BindingStrict] an unbound
// message is rejected with [ErrUnbound], and a bound one with a namespace other
// than the one given through [ExpectNamespace] with [ErrNamespaceMismatch], or
// either with [ErrUnknownKey] when its KEK is not held, all before any KEK call.
//
// Exactly one [EnvelopeEvent], with no namespace, is reported to the Observer
// on every path including failures.
func (v *Vault) Open(ctx context.Context, msg *Message, opts ...OpenOption) ([]byte, error) {
	var oo openOptions
	for _, opt := range opts {
		opt(&oo)
	}

	start := time.Now()
	pt, step, err := v.open(ctx, msg, oo)

	v.observer.Observe(EnvelopeEvent{
		Op:              OpDecrypt,
		Err:             err,
		CryptoAttempted: step.ran,
		CryptoErr:       step.err,
		Total:           time.Since(start),
		Crypto:          step.dur,
	})

	return pt, err
}

// Refresh rotates every namespace DEK that has reached its renewal threshold.
// It is meant to be called periodically to keep rotation off the request path;
// Seal rotates an expired DEK on demand, so calling it is optional. One
// [RotationEvent] with [RotationScheduled] is reported per key rotated.
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

	// Collect what actually rotated under the lock and report it after the
	// closure returns and its deferred unlock has run: an Observer must never
	// run while v.mu is held, or a slow one stalls every Seal and one that
	// calls back into the Vault deadlocks. The locked region is a closure
	// rather than inline code so a panic partway through still unlocks; an
	// explicit Unlock placed after the loop would skip that on a panic and
	// leave v.mu write-locked forever.
	rotated := func() []string {
		v.mu.Lock()
		defer v.mu.Unlock()

		rotated := make([]string, 0, len(updates))
		for k, d := range updates {
			// Re-check: a concurrent Seal (or Refresh) may have rotated this key
			// between the scan above and this write lock. Rotating again would
			// discard that fresh DEK and force an extra KEK wrap on the next Seal.
			if v.keys[k].isExpired(v.nowFn) {
				v.keys[k].rotate(d, v.nowFn)
				rotated = append(rotated, k)
			}
		}

		return rotated
	}()

	for _, ns := range rotated {
		v.observer.Observe(RotationEvent{Namespace: ns, Reason: RotationScheduled})
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

// seal is Seal's body. The middle return value describes what the AES-256-GCM
// step cost and whether it failed, or is the zero value if it was never
// reached.
func (v *Vault) seal(ctx context.Context, ns, label string, data []byte) (*Message, cryptoStep, error) {
	sd, err := v.getOrRefreshKey(ns)
	if err != nil {
		return nil, cryptoStep{}, err
	}

	// Snapshot key and material under RLock; release immediately so the
	// encryption work happens outside the lock.
	v.mu.RLock()
	dek := sd.key
	material := sd.material
	v.mu.RUnlock()

	bound := v.binding != BindingOff
	out := &Message{Bound: bound}
	var aad []byte
	if bound {
		out.Namespace = label
		aad = namespaceAAD(label)
	}

	cryptoStart := time.Now()
	ct, err := dek.Encrypt(ctx, data, aad)
	step := cryptoStep{ran: true, dur: time.Since(cryptoStart), err: err}
	if err != nil {
		return nil, step, err
	}
	out.Ciphertext = ct

	if material != nil {
		out.KeyMaterial = material
		return out, step, nil
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
		return nil, step, err
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

	out.KeyMaterial = m
	return out, step, nil
}

// open is Open's body. The middle return value describes what the AES-256-GCM
// step cost and whether it failed, or is the zero value if it was never
// reached.
func (v *Vault) open(ctx context.Context, msg *Message, oo openOptions) ([]byte, cryptoStep, error) {
	if msg == nil || msg.KeyMaterial == nil {
		return nil, cryptoStep{}, errors.New("message and key material must not be nil")
	}

	// Rejected before the cache and the KEK, so a refused message costs no KMS
	// call and a cached DEK cannot let one through. The KEK check comes first so
	// a message sealed under a key this Vault lacks still reports ErrUnknownKey,
	// which a chained proxy passes through.
	if v.binding == BindingStrict {
		var refusal error
		switch {
		case !msg.Bound:
			refusal = ErrUnbound
		case oo.hasExpect && msg.Namespace != oo.expect:
			refusal = ErrNamespaceMismatch
		}

		if refusal != nil {
			if !v.registry.holds(msg.KeyMaterial.KEKID) {
				return nil, cryptoStep{}, fmt.Errorf("%w: %s", ErrUnknownKey, msg.KeyMaterial.KEKID)
			}

			return nil, cryptoStep{}, refusal
		}
	}

	var dek *DEK
	if v.cache != nil {
		if cached, ok := v.cache.Get(msg.KeyMaterial.EncryptedDEK); ok {
			dek = cached
			v.observer.Observe(CacheEvent{Hit: true, Size: v.cache.Len()})
		} else {
			v.observer.Observe(CacheEvent{Hit: false, Size: v.cache.Len()})
		}
	}

	if dek == nil {
		var err error
		dek, err = v.registry.Decrypt(ctx, msg.KeyMaterial)
		if err != nil {
			return nil, cryptoStep{}, err
		}

		if v.cache != nil {
			v.cache.Add(msg.KeyMaterial.EncryptedDEK, dek)
		}
	}

	var aad []byte
	if msg.Bound {
		aad = namespaceAAD(msg.Namespace)
	}

	cryptoStart := time.Now()
	pt, err := dek.Decrypt(ctx, msg.Ciphertext, aad)

	return pt, cryptoStep{ran: true, dur: time.Since(cryptoStart), err: err}, err
}

// getOrRefreshKey returns the sliding DEK for ns, creating or rotating it on
// demand, and reports any rotation to the Observer. The report happens here
// rather than inside lookupKey so that it lands after lookupKey's deferred
// unlock: an Observer must never run while v.mu is held.
func (v *Vault) getOrRefreshKey(ns string) (*slidingDEK, error) {
	sd, rotated, err := v.lookupKey(ns)
	if rotated != nil {
		v.observer.Observe(*rotated)
	}

	return sd, err
}

// lookupKey is getOrRefreshKey's body. The middle return value describes a
// rotation it performed, or nil if the key it returns was already current. Any
// event it produces, including one bubbled up from createDefaultKey, is reported
// by its caller.
func (v *Vault) lookupKey(ns string) (*slidingDEK, *RotationEvent, error) {
	v.mu.RLock()
	key := v.keys[ns]
	if key == nil {
		v.mu.RUnlock()
		if v.defaultConfig == nil {
			return nil, nil, fmt.Errorf("key not found, ns: %s", ns)
		}

		return v.createDefaultKey(ns)
	}

	if !key.isExpired(v.nowFn) {
		v.mu.RUnlock()
		return key, nil, nil
	}
	v.mu.RUnlock()

	// Ideally we wouldn't get here. This is the case when Refresh hasn't been called, but the key is expired.
	v.mu.Lock()
	defer v.mu.Unlock()

	// Re-check: Refresh (or another Seal) may have already rotated this key. It
	// reports nothing, so a contended rotation yields exactly one event.
	if !key.isExpired(v.nowFn) {
		return key, nil, nil
	}

	k, err := NewDEK()
	if err != nil {
		return nil, nil, fmt.Errorf("failed to generate DEK for %s: %w", ns, err)
	}

	key.rotate(k, v.nowFn)

	return key, &RotationEvent{Namespace: ns, Reason: RotationOnDemand}, nil
}

func (v *Vault) createDefaultKey(ns string) (*slidingDEK, *RotationEvent, error) {
	v.mu.Lock()
	defer v.mu.Unlock()

	// Re-check: another goroutine may have created this slot while we waited for
	// the lock. It reports nothing, so a namespace is only ever announced once.
	if key := v.keys[ns]; key != nil {
		return key, nil, nil
	}

	dek, err := NewDEK()
	if err != nil {
		return nil, nil, fmt.Errorf("failed to generate DEK for %s: %w", ns, err)
	}

	sd := &slidingDEK{
		dur:    v.defaultConfig.Duration,
		before: v.defaultConfig.RenewBefore,
	}

	sd.rotate(dek, v.nowFn)
	v.keys[ns] = sd

	return sd, &RotationEvent{Namespace: ns, Reason: RotationInitial}, nil
}

// Seal encrypts data within the bound namespace. See [Vault.Seal].
func (v *NamespacedVault) Seal(ctx context.Context, data []byte, opts ...SealOption) (*Message, error) {
	return v.inner.Seal(ctx, v.ns, data, opts...)
}

// Open decrypts msg by delegating to the inner vault. The bound namespace is
// unused: the key is selected from msg's key material, and any namespace
// binding is read from msg itself. See [Vault.Open].
func (v *NamespacedVault) Open(ctx context.Context, msg *Message, opts ...OpenOption) ([]byte, error) {
	return v.inner.Open(ctx, msg, opts...)
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

// namespaceAAD is the additional data binding a payload to ns. The version and
// separator keep it prefix-free, and an empty ns still yields non-empty bytes,
// so bound-to-empty never matches unbound. It is permanent: every bound payload
// authenticates against these bytes, so a change needs a new version string,
// never an edit to this one.
func namespaceAAD(ns string) []byte {
	const prefix = "temporal-proxy/ns/v1\x00"

	aad := make([]byte, 0, len(prefix)+len(ns))
	aad = append(aad, prefix...)

	return append(aad, ns...)
}
