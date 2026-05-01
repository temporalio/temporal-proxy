package crypto_test

import (
	"context"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/temporalio/temporal-proxy/internal/crypto"
)

type (
	// testKEK is a passthrough KEK that returns key material unchanged.
	testKEK struct{ id string }

	// channelKEK wraps countingKEK with an optional gate channel. When gate is
	// non-nil, Encrypt blocks until it is closed, letting tests control exactly
	// when concurrent KMS calls are allowed to complete.
	channelKEK struct {
		countingKEK
		mu   sync.Mutex
		gate chan struct{}
	}

	countingKEK struct {
		*testKEK
		encrypts atomic.Int64
		decrypts atomic.Int64
	}
)

func TestSealer_SealOpen(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		data []byte
	}{
		{"empty", nil},
		{"small", []byte("hello world")},
		{"large", make([]byte, 64*1024)},
	}

	cfg := crypto.KeyConfig{Lifetime: time.Hour, ExpirationBuffer: time.Minute}
	s := newTestSealer(t, "ns", cfg)

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			env, err := s.Seal(t.Context(), "ns", tc.data)
			require.NoError(t, err)

			got, err := s.Open(t.Context(), env)
			require.NoError(t, err)
			require.Equal(t, tc.data, got)
		})
	}
}

func TestSealer_Seal_UnknownID(t *testing.T) {
	t.Parallel()

	s, err := crypto.NewSealer(crypto.NewKEKRegistry())
	require.NoError(t, err)

	_, err = s.Seal(t.Context(), "unknown", []byte("data"))
	require.Error(t, err)
}

func TestSealer_Open_TamperedCipherText(t *testing.T) {
	t.Parallel()

	cfg := crypto.KeyConfig{Lifetime: time.Hour, ExpirationBuffer: time.Minute}
	s := newTestSealer(t, "ns", cfg)

	env, err := s.Seal(t.Context(), "ns", []byte("hello"))
	require.NoError(t, err)

	env.CipherText[0] ^= 0xFF
	_, err = s.Open(t.Context(), env)
	require.Error(t, err)
}

func TestSealer_Refresh_RotatesExpiredKeys(t *testing.T) {
	t.Parallel()

	now, timeTravel := newClock(time.Now())
	cfg := crypto.KeyConfig{Lifetime: time.Hour, ExpirationBuffer: 10 * time.Minute}
	s := newTestSealer(t, "ns", cfg, crypto.WithNowFunc(now))

	env1, err := s.Seal(t.Context(), "ns", []byte("before rotation"))
	require.NoError(t, err)

	// Advance past the expiration buffer threshold (60m lifetime - 10m buffer = expires at 50m).
	timeTravel(51 * time.Minute)
	require.NoError(t, s.Refresh())

	env2, err := s.Seal(t.Context(), "ns", []byte("after rotation"))
	require.NoError(t, err)

	// Both envelopes must still be openable after key rotation.
	got1, err := s.Open(t.Context(), env1)
	require.NoError(t, err)
	require.Equal(t, []byte("before rotation"), got1)

	got2, err := s.Open(t.Context(), env2)
	require.NoError(t, err)
	require.Equal(t, []byte("after rotation"), got2)
}

func TestSealer_Refresh_NoOp_WhenNotExpired(t *testing.T) {
	t.Parallel()

	now, _ := newClock(time.Now())
	cfg := crypto.KeyConfig{Lifetime: time.Hour, ExpirationBuffer: 10 * time.Minute}
	s := newTestSealer(t, "ns", cfg, crypto.WithNowFunc(now))

	env, err := s.Seal(t.Context(), "ns", []byte("hello"))
	require.NoError(t, err)

	require.NoError(t, s.Refresh())

	got, err := s.Open(t.Context(), env)
	require.NoError(t, err)
	require.Equal(t, []byte("hello"), got)
}

func TestSealer_Seal_RotatesExpiredKeyInline(t *testing.T) {
	t.Parallel()

	// Verify that Seal handles an expired key even when Refresh hasn't been called.
	now, timeTravel := newClock(time.Now())
	cfg := crypto.KeyConfig{Lifetime: time.Hour, ExpirationBuffer: 10 * time.Minute}
	s := newTestSealer(t, "ns", cfg, crypto.WithNowFunc(now))

	env1, err := s.Seal(t.Context(), "ns", []byte("before"))
	require.NoError(t, err)

	timeTravel(51 * time.Minute) // expire without calling Refresh

	env2, err := s.Seal(t.Context(), "ns", []byte("after"))
	require.NoError(t, err)

	got1, err := s.Open(t.Context(), env1)
	require.NoError(t, err)
	require.Equal(t, []byte("before"), got1)

	got2, err := s.Open(t.Context(), env2)
	require.NoError(t, err)
	require.Equal(t, []byte("after"), got2)
}

func TestSealer_Seal_CachesKeyMaterial(t *testing.T) {
	t.Parallel()

	cfg := crypto.KeyConfig{Lifetime: time.Hour, ExpirationBuffer: time.Minute}
	kek := &countingKEK{testKEK: &testKEK{id: "kek-ns"}}
	reg := crypto.NewKEKRegistry(crypto.WithKeyForNamespace("ns", kek))
	s, err := crypto.NewSealer(reg, crypto.WithKeyConfig("ns", cfg))
	require.NoError(t, err)

	_, err = s.Seal(t.Context(), "ns", []byte("first"))
	require.NoError(t, err)
	_, err = s.Seal(t.Context(), "ns", []byte("second"))
	require.NoError(t, err)

	require.Equal(t, int64(1), kek.encrypts.Load(), "second Seal should reuse cached DEKMaterial, not call KMS again")
}

func TestSealer_Open_CachesDecryptedDEK(t *testing.T) {
	t.Parallel()

	cfg := crypto.KeyConfig{Lifetime: time.Hour, ExpirationBuffer: time.Minute}
	kek := &countingKEK{testKEK: &testKEK{id: "kek-ns"}}
	reg := crypto.NewKEKRegistry(crypto.WithKeyForNamespace("ns", kek))
	s, err := crypto.NewSealer(reg,
		crypto.WithKeyConfig("ns", cfg),
		crypto.WithCacheSize(10),
	)
	require.NoError(t, err)

	env, err := s.Seal(t.Context(), "ns", []byte("hello"))
	require.NoError(t, err)

	_, err = s.Open(t.Context(), env)
	require.NoError(t, err)
	afterFirst := kek.decrypts.Load()

	_, err = s.Open(t.Context(), env)
	require.NoError(t, err)
	require.Equal(t, afterFirst, kek.decrypts.Load(), "second Open must hit cache, not KMS")
}

func TestSealer_Open_CacheEviction(t *testing.T) {
	t.Parallel()

	now, timeTravel := newClock(time.Now())
	cfg := crypto.KeyConfig{Lifetime: time.Hour, ExpirationBuffer: 10 * time.Minute}
	kek := &countingKEK{testKEK: &testKEK{id: "kek-ns"}}
	reg := crypto.NewKEKRegistry(crypto.WithKeyForNamespace("ns", kek))
	s, err := crypto.NewSealer(reg,
		crypto.WithKeyConfig("ns", cfg),
		crypto.WithCacheSize(2),
		crypto.WithNowFunc(now),
	)
	require.NoError(t, err)

	// Seal with DEK1.
	env1, err := s.Seal(t.Context(), "ns", []byte("data1"))
	require.NoError(t, err)

	// Rotate to DEK2.
	timeTravel(51 * time.Minute)
	require.NoError(t, s.Refresh())

	// Seal with DEK2.
	env2, err := s.Seal(t.Context(), "ns", []byte("data2"))
	require.NoError(t, err)

	// Rotate to DEK3.
	timeTravel(51 * time.Minute)
	require.NoError(t, s.Refresh())

	// Seal with DEK3.
	env3, err := s.Seal(t.Context(), "ns", []byte("data3"))
	require.NoError(t, err)

	// Open all three in order: cache fills to capacity 2 then evicts DEK1 (LRU).
	// Open env1 → miss → KMS → cache: [DEK1]
	// Open env2 → miss → KMS → cache: [DEK1, DEK2]
	// Open env3 → miss → KMS → cache: [DEK2, DEK3] (DEK1 evicted)
	_, err = s.Open(t.Context(), env1)
	require.NoError(t, err)
	_, err = s.Open(t.Context(), env2)
	require.NoError(t, err)
	_, err = s.Open(t.Context(), env3)
	require.NoError(t, err)

	decryptsBefore := kek.decrypts.Load()

	// Re-open env1 — DEK1 was evicted, must hit KMS again.
	_, err = s.Open(t.Context(), env1)
	require.NoError(t, err)
	require.Equal(t, decryptsBefore+1, kek.decrypts.Load(), "evicted DEK must trigger a fresh KMS call")
}

func TestSealer_Open_NoCacheWhenSizeZero(t *testing.T) {
	t.Parallel()

	cfg := crypto.KeyConfig{Lifetime: time.Hour, ExpirationBuffer: time.Minute}
	kek := &countingKEK{testKEK: &testKEK{id: "kek-ns"}}
	reg := crypto.NewKEKRegistry(crypto.WithKeyForNamespace("ns", kek))
	s, err := crypto.NewSealer(reg,
		crypto.WithKeyConfig("ns", cfg),
		crypto.WithCacheSize(0), // explicitly disable the default cache
	)
	require.NoError(t, err)

	env, err := s.Seal(t.Context(), "ns", []byte("hello"))
	require.NoError(t, err)

	_, err = s.Open(t.Context(), env)
	require.NoError(t, err)
	_, err = s.Open(t.Context(), env)
	require.NoError(t, err)

	require.Equal(t, int64(2), kek.decrypts.Load(), "without a cache every Open must call KMS")
}

func TestSealer_Open_DefaultCacheEnabled(t *testing.T) {
	t.Parallel()

	cfg := crypto.KeyConfig{Lifetime: time.Hour, ExpirationBuffer: time.Minute}
	kek := &countingKEK{testKEK: &testKEK{id: "kek-ns"}}
	reg := crypto.NewKEKRegistry(crypto.WithKeyForNamespace("ns", kek))
	s, err := crypto.NewSealer(reg, crypto.WithKeyConfig("ns", cfg))
	require.NoError(t, err)

	env, err := s.Seal(t.Context(), "ns", []byte("hello"))
	require.NoError(t, err)

	_, err = s.Open(t.Context(), env)
	require.NoError(t, err)
	after := kek.decrypts.Load()

	_, err = s.Open(t.Context(), env)
	require.NoError(t, err)
	require.Equal(t, after, kek.decrypts.Load(), "default cache should serve second Open without KMS")
}

func TestSealer_Seal_CoalescesKMSCalls(t *testing.T) {
	t.Parallel()

	const n = 50
	cfg := crypto.KeyConfig{Lifetime: time.Hour, ExpirationBuffer: time.Minute}
	kek := &channelKEK{countingKEK: countingKEK{testKEK: &testKEK{id: "kek-ns"}}}
	reg := crypto.NewKEKRegistry(crypto.WithKeyForNamespace("ns", kek))
	s, err := crypto.NewSealer(reg, crypto.WithKeyConfig("ns", cfg))
	require.NoError(t, err)

	// Install the gate before releasing goroutines so the KMS call blocks
	// until all callers have joined the singleflight group.
	gate := kek.openGate()
	ready := make(chan struct{})

	var wg sync.WaitGroup
	wg.Add(n)
	for range n {
		go func() {
			defer wg.Done()
			<-ready
			_, serr := s.Seal(t.Context(), "ns", []byte("data"))
			require.NoError(t, serr)
		}()
	}
	close(ready)

	// Spin until the first goroutine has entered kek.Encrypt (gate is now held).
	for kek.encrypts.Load() == 0 {
		runtime.Gosched()
	}

	// Give the remaining goroutines time to reach sf.Do and join the in-flight group.
	time.Sleep(5 * time.Millisecond)
	close(gate)
	wg.Wait()

	require.Equal(t, int64(1), kek.encrypts.Load(), "concurrent first-seal burst must produce exactly 1 KMS call")
}

func TestSealer_Seal_CoalescesKMSCalls_AfterRotation(t *testing.T) {
	t.Parallel()

	const n = 50
	now, timeTravel := newClock(time.Now())
	cfg := crypto.KeyConfig{Lifetime: time.Hour, ExpirationBuffer: 10 * time.Minute}
	kek := &channelKEK{countingKEK: countingKEK{testKEK: &testKEK{id: "kek-ns"}}}
	reg := crypto.NewKEKRegistry(crypto.WithKeyForNamespace("ns", kek))
	s, err := crypto.NewSealer(reg,
		crypto.WithKeyConfig("ns", cfg),
		crypto.WithNowFunc(now),
	)
	require.NoError(t, err)

	// Initial seal: no gate, completes immediately.
	_, err = s.Seal(t.Context(), "ns", []byte("before"))
	require.NoError(t, err)
	require.Equal(t, int64(1), kek.encrypts.Load())

	timeTravel(51 * time.Minute)
	require.NoError(t, s.Refresh())

	// Install gate before the burst so the post-rotation KMS call blocks.
	gate := kek.openGate()
	ready := make(chan struct{})

	var wg sync.WaitGroup
	wg.Add(n)
	for range n {
		go func() {
			defer wg.Done()
			<-ready

			_, serr := s.Seal(t.Context(), "ns", []byte("after"))
			require.NoError(t, serr)
		}()
	}
	close(ready)

	// Spin until the first goroutine has entered kek.Encrypt (gate is now held).
	for kek.encrypts.Load() == 1 {
		runtime.Gosched()
	}

	// Give the remaining goroutines time to reach sf.Do and join the in-flight group.
	time.Sleep(5 * time.Millisecond)
	close(gate)
	wg.Wait()

	require.Equal(t, int64(2), kek.encrypts.Load(), "post-rotation burst must produce exactly 1 new KMS call")
}

func TestSealer_Seal_CacheClearedOnRotation(t *testing.T) {
	t.Parallel()

	now, timeTravel := newClock(time.Now())
	cfg := crypto.KeyConfig{Lifetime: time.Hour, ExpirationBuffer: 10 * time.Minute}
	kek := &countingKEK{testKEK: &testKEK{id: "kek-ns"}}
	reg := crypto.NewKEKRegistry(crypto.WithKeyForNamespace("ns", kek))
	s, err := crypto.NewSealer(reg,
		crypto.WithKeyConfig("ns", cfg),
		crypto.WithNowFunc(now),
	)
	require.NoError(t, err)

	// First seal populates the material cache (1 KMS call).
	_, err = s.Seal(t.Context(), "ns", []byte("before"))
	require.NoError(t, err)
	require.Equal(t, int64(1), kek.encrypts.Load())

	// Rotate the DEK.
	timeTravel(51 * time.Minute)
	require.NoError(t, s.Refresh())

	// First seal after rotation must call KMS again (2 total).
	_, err = s.Seal(t.Context(), "ns", []byte("after"))
	require.NoError(t, err)
	require.Equal(t, int64(2), kek.encrypts.Load())

	// Second seal after rotation must reuse the new cached material.
	_, err = s.Seal(t.Context(), "ns", []byte("still after"))
	require.NoError(t, err)
	require.Equal(t, int64(2), kek.encrypts.Load())
}

func TestSealer_Seal_RotationDuringSlowKMS(t *testing.T) {
	t.Parallel()

	now, timeTravel := newClock(time.Now())
	cfg := crypto.KeyConfig{Lifetime: time.Hour, ExpirationBuffer: 10 * time.Minute}
	kek := &channelKEK{countingKEK: countingKEK{testKEK: &testKEK{id: "kek-ns"}}}
	reg := crypto.NewKEKRegistry(crypto.WithKeyForNamespace("ns", kek))
	s, err := crypto.NewSealer(reg,
		crypto.WithKeyConfig("ns", cfg),
		crypto.WithNowFunc(now),
	)
	require.NoError(t, err)

	// Block all KMS calls so we can rotate the DEK while one is in-flight.
	gate := kek.openGate()

	var (
		env1 *crypto.Envelope
		env2 *crypto.Envelope
		wg   sync.WaitGroup
	)

	// G1: Seal with DEK1 — will block inside KMS.
	wg.Go(func() {
		var serr error
		env1, serr = s.Seal(t.Context(), "ns", []byte("dek1 data"))
		require.NoError(t, serr)
	})

	// Wait until G1 has entered the KMS call.
	for kek.encrypts.Load() == 0 {
		runtime.Gosched()
	}

	// Rotate the DEK while G1's KMS call is still blocked.
	timeTravel(51 * time.Minute)
	require.NoError(t, s.Refresh())

	// G2: Seal with DEK2 — must open its own singleflight group, not join G1's.
	wg.Go(func() {
		var serr error
		env2, serr = s.Seal(t.Context(), "ns", []byte("dek2 data"))
		require.NoError(t, serr)
	})

	// Wait until G2 has also entered the KMS call (two concurrent in-flight calls).
	for kek.encrypts.Load() < 2 {
		runtime.Gosched()
	}

	close(gate)
	wg.Wait()

	require.Equal(t, int64(2), kek.encrypts.Load(), "each DEK must produce its own KMS call")

	got1, err := s.Open(t.Context(), env1)
	require.NoError(t, err)
	require.Equal(t, []byte("dek1 data"), got1)

	got2, err := s.Open(t.Context(), env2)
	require.NoError(t, err)
	require.Equal(t, []byte("dek2 data"), got2)
}

// newClock returns a controllable now function and an timeTravel function for tests.
func newClock(start time.Time) (func() time.Time, func(time.Duration)) {
	var mu sync.Mutex
	cur := start
	return func() time.Time {
			mu.Lock()
			defer mu.Unlock()
			return cur
		}, func(d time.Duration) {
			mu.Lock()
			defer mu.Unlock()
			cur = cur.Add(d)
		}
}

func newTestSealer(t *testing.T, ns string, cfg crypto.KeyConfig, opts ...crypto.SealerOption) *crypto.Sealer {
	t.Helper()

	reg := crypto.NewKEKRegistry(crypto.WithKeyForNamespace(ns, &testKEK{id: "kek-" + ns}))
	s, err := crypto.NewSealer(reg, append([]crypto.SealerOption{
		crypto.WithKeyConfig(ns, cfg),
	}, opts...)...)
	require.NoError(t, err)
	return s
}

func (k *testKEK) ID() string                                           { return k.id }
func (k *testKEK) Encrypt(_ context.Context, pt []byte) ([]byte, error) { return pt, nil }
func (k *testKEK) Decrypt(_ context.Context, ct []byte) ([]byte, error) { return ct, nil }
func (k *testKEK) Close() error                                         { return nil }

func (k *countingKEK) Encrypt(ctx context.Context, pt []byte) ([]byte, error) {
	k.encrypts.Add(1)
	return k.testKEK.Encrypt(ctx, pt)
}

func (k *countingKEK) Decrypt(ctx context.Context, ct []byte) ([]byte, error) {
	k.decrypts.Add(1)
	return k.testKEK.Decrypt(ctx, ct)
}

// openGate installs a new (closed-by-caller) gate and returns it.
// Call close(gate) to unblock any in-flight Encrypt calls.
func (k *channelKEK) openGate() chan struct{} {
	ch := make(chan struct{})
	k.mu.Lock()
	k.gate = ch
	k.mu.Unlock()
	return ch
}

func (k *channelKEK) Encrypt(ctx context.Context, pt []byte) ([]byte, error) {
	k.encrypts.Add(1)
	k.mu.Lock()
	gate := k.gate
	k.mu.Unlock()
	if gate != nil {
		select {
		case <-gate:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return k.testKEK.Encrypt(ctx, pt)
}
