package crypto_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/temporalio/temporal-proxy/internal/crypto"
)

// testKEK is a passthrough KEK that returns key material unchanged.
type testKEK struct{ id string }

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

			env, err := s.Seal(context.Background(), "ns", tc.data)
			require.NoError(t, err)

			got, err := s.Open(context.Background(), env)
			require.NoError(t, err)
			require.Equal(t, tc.data, got)
		})
	}
}

func TestSealer_Seal_UnknownID(t *testing.T) {
	t.Parallel()

	s, err := crypto.NewSealer(crypto.NewKEKRegistry())
	require.NoError(t, err)

	_, err = s.Seal(context.Background(), "unknown", []byte("data"))
	require.Error(t, err)
}

func TestSealer_Open_TamperedCipherText(t *testing.T) {
	t.Parallel()

	cfg := crypto.KeyConfig{Lifetime: time.Hour, ExpirationBuffer: time.Minute}
	s := newTestSealer(t, "ns", cfg)

	env, err := s.Seal(context.Background(), "ns", []byte("hello"))
	require.NoError(t, err)

	env.CipherText[0] ^= 0xFF
	_, err = s.Open(context.Background(), env)
	require.Error(t, err)
}

func TestSealer_Refresh_RotatesExpiredKeys(t *testing.T) {
	t.Parallel()

	now, timeTravel := newClock(time.Now())
	cfg := crypto.KeyConfig{Lifetime: time.Hour, ExpirationBuffer: 10 * time.Minute}
	s := newTestSealer(t, "ns", cfg, crypto.WithNowFunc(now))

	env1, err := s.Seal(context.Background(), "ns", []byte("before rotation"))
	require.NoError(t, err)

	// Advance past the expiration buffer threshold (60m lifetime - 10m buffer = expires at 50m).
	timeTravel(51 * time.Minute)
	require.NoError(t, s.Refresh())

	env2, err := s.Seal(context.Background(), "ns", []byte("after rotation"))
	require.NoError(t, err)

	// Both envelopes must still be openable after key rotation.
	got1, err := s.Open(context.Background(), env1)
	require.NoError(t, err)
	require.Equal(t, []byte("before rotation"), got1)

	got2, err := s.Open(context.Background(), env2)
	require.NoError(t, err)
	require.Equal(t, []byte("after rotation"), got2)
}

func TestSealer_Refresh_NoOp_WhenNotExpired(t *testing.T) {
	t.Parallel()

	now, _ := newClock(time.Now())
	cfg := crypto.KeyConfig{Lifetime: time.Hour, ExpirationBuffer: 10 * time.Minute}
	s := newTestSealer(t, "ns", cfg, crypto.WithNowFunc(now))

	env, err := s.Seal(context.Background(), "ns", []byte("hello"))
	require.NoError(t, err)

	require.NoError(t, s.Refresh())

	got, err := s.Open(context.Background(), env)
	require.NoError(t, err)
	require.Equal(t, []byte("hello"), got)
}

func TestSealer_Seal_RotatesExpiredKeyInline(t *testing.T) {
	t.Parallel()

	// Verify that Seal handles an expired key even when Refresh hasn't been called.
	now, timeTravel := newClock(time.Now())
	cfg := crypto.KeyConfig{Lifetime: time.Hour, ExpirationBuffer: 10 * time.Minute}
	s := newTestSealer(t, "ns", cfg, crypto.WithNowFunc(now))

	env1, err := s.Seal(context.Background(), "ns", []byte("before"))
	require.NoError(t, err)

	timeTravel(51 * time.Minute) // expire without calling Refresh

	env2, err := s.Seal(context.Background(), "ns", []byte("after"))
	require.NoError(t, err)

	got1, err := s.Open(context.Background(), env1)
	require.NoError(t, err)
	require.Equal(t, []byte("before"), got1)

	got2, err := s.Open(context.Background(), env2)
	require.NoError(t, err)
	require.Equal(t, []byte("after"), got2)
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
