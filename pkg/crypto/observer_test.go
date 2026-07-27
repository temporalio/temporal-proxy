package crypto_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/temporalio/temporal-proxy/pkg/crypto"
)

type spyObserver struct {
	hits   []int
	misses []int
}

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

	require.Equal(t, []int{0}, spy.misses)
	require.Equal(t, []int{1}, spy.hits)
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

	require.Empty(t, spy.hits)
	require.Empty(t, spy.misses)
}

func TestObserverDefaultNoPanic(t *testing.T) {
	t.Parallel()

	v := newTestVault(t) // no WithObserver

	msg, err := v.Seal(t.Context(), "ns1", []byte("hello"))
	require.NoError(t, err)
	_, err = v.Open(t.Context(), msg)
	require.NoError(t, err) // must not panic on the nil-default path
}

func (s *spyObserver) CacheHit(e crypto.CacheEvent)  { s.hits = append(s.hits, e.Size) }
func (s *spyObserver) CacheMiss(e crypto.CacheEvent) { s.misses = append(s.misses, e.Size) }

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
