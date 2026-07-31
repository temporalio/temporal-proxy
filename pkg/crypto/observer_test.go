package crypto_test

import (
	"bytes"
	"context"
	"errors"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/temporalio/temporal-proxy/pkg/crypto"
)

// reentrancyDeadline bounds how long a reentrant Seal is allowed to take.
// Generous enough not to flake on a loaded machine, but far faster and far
// clearer than waiting for the package timeout to report a deadlock.
const reentrancyDeadline = 5 * time.Second

type (
	spyObserver struct {
		mu        sync.Mutex
		hits      []int
		misses    []int
		ops       []crypto.EnvelopeEvent
		rotations []crypto.RotationEvent
	}

	// reentrantRefreshObserver seals through the Vault from inside the
	// RotationScheduled callback fired by Refresh, which deadlocks if Refresh
	// reports while still holding v.mu. It reenters once only: the nested Seal
	// can itself rotate, and unbounded recursion would mask the deadlock this
	// exists to detect.
	//
	// fired is a CompareAndSwap guard rather than a sync.Once, and rather than
	// the mutex-guarded bool this codebase would otherwise prefer, because both
	// of those deadlock on reentry. sync.Once.Do deadlocks by contract if f
	// causes Do to be called again on the same Once before the first call
	// returns, and a mutex fails the same way: the nested Observe would block
	// re-taking a lock the outer call still holds. An atomic is the only guard
	// here that a reentrant caller can pass through.
	reentrantRefreshObserver struct {
		vault     *crypto.Vault
		ctx       context.Context
		fired     atomic.Bool
		reentered bool
		err       error
	}

	// reentrantSealObserver seals a second, unconfigured namespace through the
	// Vault from inside the RotationInitial callback fired by createDefaultKey,
	// which deadlocks if that report runs while still holding v.mu. It reenters
	// once only, and guards that with an atomic for the same reason as
	// reentrantRefreshObserver: the nested Seal creates ns-b's own default key
	// and so reports its own RotationInitial back through this same observer,
	// which neither a sync.Once nor a mutex could tolerate.
	reentrantSealObserver struct {
		vault     *crypto.Vault
		ctx       context.Context
		fired     atomic.Bool
		reentered bool
		err       error
	}
)

func TestObserverMissThenHit(t *testing.T) {
	t.Parallel()

	spy := &spyObserver{}
	v := newTestVault(t, crypto.WithObserver(spy))

	msg, err := v.Seal(t.Context(), "ns1", []byte("hello"))
	require.NoError(t, err)

	// First open: cache empty -> miss (size before add is 0).
	pt, err := v.Open(t.Context(), msg)
	require.NoError(t, err)
	require.Equal(t, []byte("hello"), pt)

	// Second open of the same message: served from cache -> hit (size 1).
	_, err = v.Open(t.Context(), msg)
	require.NoError(t, err)

	require.Equal(t, []int{0}, spy.cacheMissSizes())
	require.Equal(t, []int{1}, spy.cacheHitSizes())
}

func TestObserverDisabledCache(t *testing.T) {
	t.Parallel()

	spy := &spyObserver{}
	v := newTestVault(t, crypto.WithObserver(spy), crypto.WithCacheSize(0))

	msg, err := v.Seal(t.Context(), "ns1", []byte("hello"))
	require.NoError(t, err)
	_, err = v.Open(t.Context(), msg)
	require.NoError(t, err)
	_, err = v.Open(t.Context(), msg)
	require.NoError(t, err)

	require.Empty(t, spy.cacheHitSizes())
	require.Empty(t, spy.cacheMissSizes())
}

func TestObserverDefaultNoPanic(t *testing.T) {
	t.Parallel()

	clock := &testClock{}
	v := newVault(t, &countingKEK{id: "default"}, // no WithObserver
		crypto.WithNowFunc(clock.Now),
		crypto.WithKeyConfig("ns1", crypto.KeyConfig{Duration: time.Hour}))

	msg, err := v.Seal(t.Context(), "ns1", []byte("hello"))
	require.NoError(t, err)

	_, err = v.Open(t.Context(), msg) // must not panic on the nil-default path
	require.NoError(t, err)

	// Reach the rotation path so the default Observer's Observe is actually
	// called with a RotationEvent.
	clock.advance(2 * time.Hour)
	require.NoError(t, v.Refresh())
}

// Observe records e, distinguishing a cache hit from a miss by e.Hit rather
// than by which method was called.
func (s *spyObserver) Observe(e crypto.Event) {
	s.mu.Lock()
	defer s.mu.Unlock()

	switch ev := e.(type) {
	case crypto.CacheEvent:
		if ev.Hit {
			s.hits = append(s.hits, ev.Size)
		} else {
			s.misses = append(s.misses, ev.Size)
		}
	case crypto.EnvelopeEvent:
		s.ops = append(s.ops, ev)
	case crypto.RotationEvent:
		s.rotations = append(s.rotations, ev)
	}
}

// reset drops everything recorded so far, so a test can set a Vault up and then
// assert on a single operation. It nils the slices rather than truncating them,
// because tests compare against a nil expectation.
func (s *spyObserver) reset() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.hits, s.misses, s.ops, s.rotations = nil, nil, nil, nil
}

func (s *spyObserver) cacheHitSizes() []int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.hits)
}

func (s *spyObserver) cacheMissSizes() []int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.misses)
}

func (s *spyObserver) envelopeOps() []crypto.EnvelopeEvent {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.ops)
}

func (s *spyObserver) rotationEvents() []crypto.RotationEvent {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.rotations)
}

func newTestVault(t *testing.T, opts ...crypto.VaultOption) *crypto.Vault {
	t.Helper()
	reg, err := crypto.NewKEKRegistry(crypto.WithDefaultKey(&fakeKEK{id: "k1"}))
	require.NoError(t, err)

	base := []crypto.VaultOption{
		crypto.WithDefaultKeyConfig(crypto.KeyConfig{Duration: time.Hour, RenewBefore: time.Minute}),
	}
	v, err := crypto.NewVault(reg, append(base, opts...)...)
	require.NoError(t, err)
	return v
}

func TestOperationString(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		op   crypto.Operation
		want string
	}{
		{name: "encrypt", op: crypto.OpEncrypt, want: "encrypt"},
		{name: "decrypt", op: crypto.OpDecrypt, want: "decrypt"},
		{name: "out of range", op: crypto.Operation(99), want: "unknown"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tt.want, tt.op.String())
		})
	}
}

func TestRotationReasonString(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		reason crypto.RotationReason
		want   string
	}{
		{name: "scheduled", reason: crypto.RotationScheduled, want: "scheduled"},
		{name: "on demand", reason: crypto.RotationOnDemand, want: "on_demand"},
		{name: "initial", reason: crypto.RotationInitial, want: "initial"},
		{name: "out of range", reason: crypto.RotationReason(99), want: "unknown"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tt.want, tt.reason.String())
		})
	}
}

func TestObserverRotationEvents(t *testing.T) {
	t.Parallel()

	// Expiry follows the injected clock while durations follow time.Now, so
	// jumping the clock forces a rotation without disturbing any timing.
	newClockedVault := func(t *testing.T, spy *spyObserver, clock *testClock) *crypto.Vault {
		t.Helper()
		return newVault(t, &countingKEK{id: "default"},
			crypto.WithObserver(spy),
			crypto.WithNowFunc(clock.Now),
			crypto.WithKeyConfig("ns1", crypto.KeyConfig{Duration: time.Hour}))
	}

	tests := []struct {
		name string
		run  func(t *testing.T, spy *spyObserver)
		want []crypto.RotationEvent
	}{
		{
			name: "refresh rotates an expired key",
			run: func(t *testing.T, spy *spyObserver) {
				clock := &testClock{}
				v := newClockedVault(t, spy, clock)

				clock.advance(2 * time.Hour)
				require.NoError(t, v.Refresh())
			},
			want: []crypto.RotationEvent{{Namespace: "ns1", Reason: crypto.RotationScheduled}},
		},
		{
			name: "refresh rotates two expired keys",
			run: func(t *testing.T, spy *spyObserver) {
				clock := &testClock{}
				v := newVault(t, &countingKEK{id: "default"},
					crypto.WithObserver(spy),
					crypto.WithNowFunc(clock.Now),
					crypto.WithKeyConfig("ns1", crypto.KeyConfig{Duration: time.Hour}),
					crypto.WithKeyConfig("ns2", crypto.KeyConfig{Duration: time.Hour}))

				clock.advance(2 * time.Hour)
				require.NoError(t, v.Refresh())
			},
			// Refresh builds its rotated list by ranging a map, so the order
			// callers observe is not part of the contract.
			want: []crypto.RotationEvent{
				{Namespace: "ns1", Reason: crypto.RotationScheduled},
				{Namespace: "ns2", Reason: crypto.RotationScheduled},
			},
		},
		{
			name: "refresh with nothing expired",
			run: func(t *testing.T, spy *spyObserver) {
				clock := &testClock{}
				v := newClockedVault(t, spy, clock)

				require.NoError(t, v.Refresh())
			},
			want: nil,
		},
		{
			name: "seal finds the key already expired",
			run: func(t *testing.T, spy *spyObserver) {
				clock := &testClock{}
				v := newClockedVault(t, spy, clock)

				clock.advance(2 * time.Hour)
				_, err := v.Seal(t.Context(), "ns1", []byte("data"))
				require.NoError(t, err)
			},
			want: []crypto.RotationEvent{{Namespace: "ns1", Reason: crypto.RotationOnDemand}},
		},
		{
			name: "first seal on an unconfigured namespace",
			run: func(t *testing.T, spy *spyObserver) {
				v := newVault(t, &countingKEK{id: "default"},
					crypto.WithObserver(spy),
					crypto.WithDefaultKeyConfig(crypto.KeyConfig{Duration: time.Hour}))

				_, err := v.Seal(t.Context(), "new-ns", []byte("data"))
				require.NoError(t, err)
			},
			want: []crypto.RotationEvent{{Namespace: "new-ns", Reason: crypto.RotationInitial}},
		},
		{
			name: "second seal on that namespace",
			run: func(t *testing.T, spy *spyObserver) {
				v := newVault(t, &countingKEK{id: "default"},
					crypto.WithObserver(spy),
					crypto.WithDefaultKeyConfig(crypto.KeyConfig{Duration: time.Hour}))

				_, err := v.Seal(t.Context(), "new-ns", []byte("data"))
				require.NoError(t, err)

				spy.reset()
				_, err = v.Seal(t.Context(), "new-ns", []byte("data"))
				require.NoError(t, err)
			},
			want: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			spy := &spyObserver{}
			tt.run(t, spy)
			require.ElementsMatch(t, tt.want, spy.rotationEvents())
		})
	}
}

func TestObserverDEKOpEvents(t *testing.T) {
	t.Parallel()

	// Large enough that the AES step is comfortably longer than the clock's
	// resolution, so the Crypto assertions test the documented invariant rather
	// than timer granularity.
	payload := bytes.Repeat([]byte("payload"), 8192)

	newSealed := func(t *testing.T, spy *spyObserver) (*crypto.Vault, *crypto.Message) {
		t.Helper()
		v := newVault(t, &countingKEK{id: "default"},
			crypto.WithObserver(spy),
			crypto.WithKeyConfig("ns1", crypto.KeyConfig{Duration: time.Hour}))

		msg, err := v.Seal(t.Context(), "ns1", payload)
		require.NoError(t, err)

		return v, msg
	}

	tests := []struct {
		name          string
		run           func(t *testing.T, spy *spyObserver)
		wantOp        crypto.Operation
		wantNS        string
		wantErr       bool
		wantCrypto    bool
		wantCryptoErr bool
	}{
		{
			name: "successful seal",
			run: func(t *testing.T, spy *spyObserver) {
				v := newVault(t, &countingKEK{id: "default"},
					crypto.WithObserver(spy),
					crypto.WithKeyConfig("ns1", crypto.KeyConfig{Duration: time.Hour}))

				_, err := v.Seal(t.Context(), "ns1", payload)
				require.NoError(t, err)
			},
			wantOp:     crypto.OpEncrypt,
			wantNS:     "ns1",
			wantCrypto: true,
		},
		{
			name: "successful open",
			run: func(t *testing.T, spy *spyObserver) {
				v, msg := newSealed(t, spy)

				spy.reset()
				_, err := v.Open(t.Context(), msg)
				require.NoError(t, err)
			},
			wantOp:     crypto.OpDecrypt,
			wantCrypto: true,
		},
		{
			name: "seal whose KEK fails to wrap",
			run: func(t *testing.T, spy *spyObserver) {
				kek := &countingKEK{id: "default", encErr: errors.New("kms unavailable")}
				v := newVault(t, kek,
					crypto.WithObserver(spy),
					crypto.WithKeyConfig("ns1", crypto.KeyConfig{Duration: time.Hour}))

				_, err := v.Seal(t.Context(), "ns1", payload)
				require.Error(t, err)
			},
			// AES ran to completion before the wrap was attempted, so Err is set
			// but CryptoErr, the AES step's own outcome, is not: this is a KEK
			// failure, not a DEK failure.
			wantOp:     crypto.OpEncrypt,
			wantNS:     "ns1",
			wantErr:    true,
			wantCrypto: true,
		},
		{
			name: "seal on an unconfigured namespace with no default config",
			run: func(t *testing.T, spy *spyObserver) {
				v := newVault(t, &countingKEK{id: "default"}, crypto.WithObserver(spy))

				_, err := v.Seal(t.Context(), "ns1", payload)
				require.Error(t, err)
			},
			// getOrRefreshKey rejects the namespace before AES is ever reached.
			wantOp:  crypto.OpEncrypt,
			wantNS:  "ns1",
			wantErr: true,
		},
		{
			name: "open with an unknown KEK id",
			run: func(t *testing.T, spy *spyObserver) {
				v, msg := newSealed(t, spy)

				// Copy before tampering: msg.KeyMaterial is the same pointer the
				// Vault holds in its sliding-key state, and mutating it in place
				// would corrupt that state for any later use of the Vault.
				tampered := *msg
				material := *msg.KeyMaterial
				material.KEKID = "nope"
				tampered.KeyMaterial = &material

				// Nothing has been opened yet, so the cache is empty and this
				// reaches the registry, which rejects the ID before AES.
				spy.reset()
				_, err := v.Open(t.Context(), &tampered)
				require.Error(t, err)
			},
			wantOp:  crypto.OpDecrypt,
			wantErr: true,
		},
		{
			name: "open with a nil message",
			run: func(t *testing.T, spy *spyObserver) {
				v := newVault(t, &countingKEK{id: "default"},
					crypto.WithObserver(spy),
					crypto.WithKeyConfig("ns1", crypto.KeyConfig{Duration: time.Hour}))

				_, err := v.Open(t.Context(), nil)
				require.Error(t, err)
			},
			wantOp:  crypto.OpDecrypt,
			wantErr: true,
		},
		{
			name: "open with tampered ciphertext",
			run: func(t *testing.T, spy *spyObserver) {
				v, msg := newSealed(t, spy)

				spy.reset()
				msg.Ciphertext[len(msg.Ciphertext)-1] ^= 0xff
				_, err := v.Open(t.Context(), msg)
				require.ErrorIs(t, err, crypto.ErrMalformedCipherText)
			},
			// AES was attempted and failed, so it still reports a duration, and
			// CryptoErr is set because the failure is AES's own.
			wantOp:        crypto.OpDecrypt,
			wantErr:       true,
			wantCrypto:    true,
			wantCryptoErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			spy := &spyObserver{}
			tt.run(t, spy)

			ops := spy.envelopeOps()
			require.Len(t, ops, 1)

			got := ops[0]
			require.Equal(t, tt.wantOp, got.Op)
			require.Equal(t, tt.wantNS, got.Namespace)
			require.GreaterOrEqual(t, got.Total, got.Crypto)

			if tt.wantErr {
				require.Error(t, got.Err)
			} else {
				require.NoError(t, got.Err)
			}

			if tt.wantCrypto {
				require.Positive(t, got.Crypto)
			} else {
				require.Zero(t, got.Crypto)
			}

			if tt.wantCryptoErr {
				require.ErrorIs(t, got.CryptoErr, crypto.ErrMalformedCipherText)
			} else {
				require.NoError(t, got.CryptoErr)
			}

			// A non-nil CryptoErr is only meaningful if AES ran, so it must imply
			// a positive Crypto duration.
			if got.CryptoErr != nil {
				require.Positive(t, got.Crypto)
			}
		})
	}
}

func TestObserverRotationReportedOutsideLockOnRefresh(t *testing.T) {
	t.Parallel()

	// An Observer must never run while the Vault holds v.mu. If it does, the
	// reentrant Seal below blocks on that lock forever: Refresh's own report
	// call is the only path in, since its own goroutine still holds the write
	// lock. The reentrant call runs on a deadline rather than in a synctest
	// bubble: synctest does not count mutex contention as durably blocked, so
	// a bubble would hang exactly as a plain test does.
	clock := &testClock{}
	obs := &reentrantRefreshObserver{ctx: t.Context()}
	v := newVault(t, &countingKEK{id: "default"},
		crypto.WithObserver(obs),
		crypto.WithNowFunc(clock.Now),
		crypto.WithKeyConfig("ns1", crypto.KeyConfig{Duration: time.Hour}),
		crypto.WithKeyConfig("ns2", crypto.KeyConfig{Duration: time.Hour}))
	obs.vault = v

	clock.advance(2 * time.Hour)

	var refreshErr error
	done := make(chan struct{})
	go func() {
		defer close(done)
		refreshErr = v.Refresh()
	}()

	select {
	case <-done:
	case <-time.After(reentrancyDeadline):
		t.Fatal("rotation reported while holding v.mu: reentrant Seal deadlocked")
	}

	require.NoError(t, refreshErr)
	require.True(t, obs.reentered)
	require.NoError(t, obs.err)
}

// Observe acts only on a RotationEvent, ignoring every other event type.
func (o *reentrantRefreshObserver) Observe(e crypto.Event) {
	if _, ok := e.(crypto.RotationEvent); !ok {
		return
	}

	if !o.fired.CompareAndSwap(false, true) {
		return
	}

	o.reentered = true
	// ns1 was just rotated, so this Seal finds a current key and does not
	// rotate again.
	_, o.err = o.vault.Seal(o.ctx, "ns1", []byte("reentrant"))
}

func TestObserverRotationReportedOutsideLockOnSeal(t *testing.T) {
	t.Parallel()

	// getOrRefreshKey's report must land after createDefaultKey's deferred
	// unlock too: a first Seal into an unconfigured namespace rotates from
	// inside createDefaultKey, not lookupKey's own rotation branch, and the
	// nested Seal below deadlocks if that report runs under the lock.
	obs := &reentrantSealObserver{ctx: t.Context()}
	v := newVault(t, &countingKEK{id: "default"},
		crypto.WithObserver(obs),
		crypto.WithDefaultKeyConfig(crypto.KeyConfig{Duration: time.Hour}))
	obs.vault = v

	var sealErr error
	done := make(chan struct{})
	go func() {
		defer close(done)
		_, sealErr = v.Seal(t.Context(), "ns-a", []byte("data"))
	}()

	select {
	case <-done:
	case <-time.After(reentrancyDeadline):
		t.Fatal("rotation reported while holding v.mu: reentrant Seal deadlocked")
	}

	require.NoError(t, sealErr)
	require.True(t, obs.reentered)
	require.NoError(t, obs.err)
}

// Observe acts only on a RotationEvent, ignoring every other event type.
func (o *reentrantSealObserver) Observe(e crypto.Event) {
	if _, ok := e.(crypto.RotationEvent); !ok {
		return
	}

	if !o.fired.CompareAndSwap(false, true) {
		return
	}

	o.reentered = true
	// ns-b is also unconfigured, so this Seal creates its own default key
	// through the same createDefaultKey path as ns-a, reporting its own
	// RotationInitial back through this same observer. The CompareAndSwap
	// above makes that a no-op instead of a second reentrant Seal.
	_, o.err = o.vault.Seal(o.ctx, "ns-b", []byte("reentrant"))
}

func TestObserverConcurrentSealEvents(t *testing.T) {
	t.Parallel()

	// One EnvelopeEvent per Seal even when callers race across a rotation boundary,
	// and no data race on the Observer itself. Also covers the singleflight
	// coalescing path with an Observer attached.
	const callers = 16

	clock := &testClock{}
	spy := &spyObserver{}
	v := newVault(t, &countingKEK{id: "default"},
		crypto.WithObserver(spy),
		crypto.WithNowFunc(clock.Now),
		crypto.WithKeyConfig("ns1", crypto.KeyConfig{Duration: time.Hour}))

	clock.advance(2 * time.Hour)

	errs := make([]error, callers)

	var wg sync.WaitGroup
	for i := range callers {
		wg.Go(func() {
			_, errs[i] = v.Seal(t.Context(), "ns1", []byte("payload"))
		})
	}
	wg.Wait()

	for _, err := range errs {
		require.NoError(t, err)
	}

	require.Len(t, spy.envelopeOps(), callers)

	// Exactly one caller wins the rotation race. Losers split into two groups:
	// those already past the read-path expiry check when the winner rotates
	// re-check under the write lock, find a current key, and report nothing;
	// those that arrive later see a current key on the RLock fast path and
	// never reach the write lock at all. Either way, only the winner reports.
	require.Len(t, spy.rotationEvents(), 1)
}
