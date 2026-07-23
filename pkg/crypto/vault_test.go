package crypto_test

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/temporalio/temporal-proxy/pkg/crypto"
)

// countingKEK is an identity KEK (ciphertext == plaintext) that counts calls.
// Because Encrypt returns the DEK bytes verbatim, a sealed Message's
// KeyMaterial.EncryptedDEK is the base64 of the DEK, so distinct DEKs produce
// distinct material and rotation is directly observable in tests.
type countingKEK struct {
	id       string
	encErr   error // when non-nil, Encrypt returns it
	encCount atomic.Int64
	decCount atomic.Int64
	block    chan struct{} // when non-nil, Encrypt waits until it is closed
}

// testClock is a race-free, manually advanced clock for driving DEK expiry.
type testClock struct{ nanos atomic.Int64 }

func TestNewVault(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		nilReg  bool
		opts    []crypto.VaultOption
		wantErr bool
	}{
		{
			name:    "nil registry",
			nilReg:  true,
			wantErr: true,
		},
		{
			name: "duplicate namespace config",
			opts: []crypto.VaultOption{
				crypto.WithKeyConfig("ns1", crypto.KeyConfig{Duration: time.Hour}),
				crypto.WithKeyConfig("ns1", crypto.KeyConfig{Duration: time.Hour}),
			},
			wantErr: true,
		},
		{
			name:    "nil now func",
			opts:    []crypto.VaultOption{crypto.WithNowFunc(nil)},
			wantErr: true,
		},
		{
			name:    "zero duration",
			opts:    []crypto.VaultOption{crypto.WithKeyConfig("ns1", crypto.KeyConfig{})},
			wantErr: true,
		},
		{
			name:    "negative renew before",
			opts:    []crypto.VaultOption{crypto.WithKeyConfig("ns1", crypto.KeyConfig{Duration: time.Hour, RenewBefore: -time.Minute})},
			wantErr: true,
		},
		{
			name:    "renew before exceeds duration",
			opts:    []crypto.VaultOption{crypto.WithKeyConfig("ns1", crypto.KeyConfig{Duration: time.Hour, RenewBefore: 2 * time.Hour})},
			wantErr: true,
		},
		{
			name:    "invalid default config",
			opts:    []crypto.VaultOption{crypto.WithDefaultKeyConfig(crypto.KeyConfig{Duration: -time.Hour})},
			wantErr: true,
		},
		{
			name: "success",
			opts: []crypto.VaultOption{crypto.WithKeyConfig("ns1", crypto.KeyConfig{Duration: time.Hour, RenewBefore: time.Minute})},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var reg *crypto.KEKRegistry
			if !tc.nilReg {
				reg = newRegistry(t)
			}

			v, err := crypto.NewVault(reg, tc.opts...)
			if tc.wantErr {
				require.Error(t, err)
				require.Nil(t, v)
				return
			}

			require.NoError(t, err)
			require.NotNil(t, v)
		})
	}
}

func TestVaultSealOpenRoundtrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		plaintext []byte
	}{
		{"empty", []byte{}},
		{"ascii", []byte("hello, world")},
		{"binary", []byte{0x00, 0xFF, 0x42, 0x13}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctx := t.Context()
			v := newVault(t, &countingKEK{id: "default"}, crypto.WithKeyConfig("ns1", crypto.KeyConfig{Duration: time.Hour}))

			msg, err := v.Seal(ctx, "ns1", tc.plaintext)
			require.NoError(t, err)
			require.NotEmpty(t, msg.KeyMaterial.EncryptedDEK)

			pt, err := v.Open(ctx, msg)
			require.NoError(t, err)
			// GCM returns a nil slice for empty plaintext, so compare by content.
			require.True(t, bytes.Equal(tc.plaintext, pt))
		})
	}
}

func TestVaultSealUnknownNamespace(t *testing.T) {
	t.Parallel()

	// No config for the namespace and no default config: sealing must fail.
	v := newVault(t, &countingKEK{id: "default"}, crypto.WithKeyConfig("ns1", crypto.KeyConfig{Duration: time.Hour}))

	_, err := v.Seal(t.Context(), "other", []byte("data"))
	require.Error(t, err)
}

func TestVaultSealDefaultConfig(t *testing.T) {
	t.Parallel()

	// With a default config, an unconfigured namespace gets a DEK on first use.
	ctx := t.Context()
	v := newVault(t, &countingKEK{id: "default"}, crypto.WithDefaultKeyConfig(crypto.KeyConfig{Duration: time.Hour}))

	msg, err := v.Seal(ctx, "brand-new", []byte("secret"))
	require.NoError(t, err)

	pt, err := v.Open(ctx, msg)
	require.NoError(t, err)
	require.Equal(t, []byte("secret"), pt)
}

func TestVaultSealReusesMaterial(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	kek := &countingKEK{id: "default"}
	v := newVault(t, kek, crypto.WithKeyConfig("ns1", crypto.KeyConfig{Duration: time.Hour}))

	first, err := v.Seal(ctx, "ns1", []byte("one"))
	require.NoError(t, err)

	second, err := v.Seal(ctx, "ns1", []byte("two"))
	require.NoError(t, err)

	// The DEK is unchanged, so its wrapped material is reused and the KEK is
	// only called once.
	require.Equal(t, int64(1), kek.encCount.Load())
	require.Equal(t, first.KeyMaterial, second.KeyMaterial)
}

func TestVaultRefresh(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	now := time.Now()
	kek := &countingKEK{id: "default"}
	v := newVault(
		t, kek,
		crypto.WithNowFunc(func() time.Time { return now }),
		crypto.WithKeyConfig("ns1", crypto.KeyConfig{Duration: time.Hour}),
	)

	before, err := v.Seal(ctx, "ns1", []byte("pre-rotation"))
	require.NoError(t, err)

	// Advance past the DEK's lifetime and rotate.
	now = now.Add(90 * time.Minute)
	require.NoError(t, v.Refresh())

	after, err := v.Seal(ctx, "ns1", []byte("post-rotation"))
	require.NoError(t, err)

	// A new DEK was generated (new wrapped material, second KEK call).
	require.NotEqual(t, before.KeyMaterial.EncryptedDEK, after.KeyMaterial.EncryptedDEK)
	require.Equal(t, int64(2), kek.encCount.Load())

	// Messages sealed with the rotated-out DEK are still openable.
	pt, err := v.Open(ctx, before)
	require.NoError(t, err)
	require.Equal(t, []byte("pre-rotation"), pt)
}

func TestVaultRefreshSkipsCurrentKeys(t *testing.T) {
	t.Parallel()

	// Refresh must be a no-op for keys that are not yet due: it leaves the DEK
	// and its wrapped material untouched and triggers no additional KEK wrap.
	ctx := t.Context()
	kek := &countingKEK{id: "default"}
	v := newVault(t, kek, crypto.WithKeyConfig("ns1", crypto.KeyConfig{Duration: time.Hour}))

	msg, err := v.Seal(ctx, "ns1", []byte("data"))
	require.NoError(t, err)
	require.Equal(t, int64(1), kek.encCount.Load())

	require.NoError(t, v.Refresh())

	next, err := v.Seal(ctx, "ns1", []byte("more"))
	require.NoError(t, err)
	require.Equal(t, msg.KeyMaterial, next.KeyMaterial)
	require.Equal(t, int64(1), kek.encCount.Load())
}

func TestVaultSealRotatesExpiredKey(t *testing.T) {
	t.Parallel()

	// Without a Refresh call, Seal rotates a DEK that has expired.
	ctx := t.Context()
	now := time.Now()
	kek := &countingKEK{id: "default"}
	v := newVault(
		t, kek,
		crypto.WithNowFunc(func() time.Time { return now }),
		crypto.WithKeyConfig("ns1", crypto.KeyConfig{Duration: time.Hour}),
	)

	before, err := v.Seal(ctx, "ns1", []byte("one"))
	require.NoError(t, err)

	now = now.Add(90 * time.Minute)

	after, err := v.Seal(ctx, "ns1", []byte("two"))
	require.NoError(t, err)

	require.NotEqual(t, before.KeyMaterial.EncryptedDEK, after.KeyMaterial.EncryptedDEK)
	require.Equal(t, int64(2), kek.encCount.Load())
}

func TestVaultOpenCache(t *testing.T) {
	t.Parallel()

	t.Run("caches decrypted DEK", func(t *testing.T) {
		t.Parallel()
		ctx := t.Context()
		kek := &countingKEK{id: "default"}
		v := newVault(t, kek, crypto.WithKeyConfig("ns1", crypto.KeyConfig{Duration: time.Hour}))

		msg, err := v.Seal(ctx, "ns1", []byte("secret"))
		require.NoError(t, err)

		for range 3 {
			pt, err := v.Open(ctx, msg)
			require.NoError(t, err)
			require.Equal(t, []byte("secret"), pt)
		}

		// The DEK is unwrapped once, then served from the cache.
		require.Equal(t, int64(1), kek.decCount.Load())
	})

	t.Run("disabled cache unwraps every open", func(t *testing.T) {
		t.Parallel()
		ctx := t.Context()
		kek := &countingKEK{id: "default"}
		v := newVault(
			t, kek,
			crypto.WithCacheSize(0),
			crypto.WithKeyConfig("ns1", crypto.KeyConfig{Duration: time.Hour}),
		)

		msg, err := v.Seal(ctx, "ns1", []byte("secret"))
		require.NoError(t, err)

		for range 3 {
			_, err := v.Open(ctx, msg)
			require.NoError(t, err)
		}

		require.Equal(t, int64(3), kek.decCount.Load())
	})
}

func TestVaultOpenErrors(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	v := newVault(t, &countingKEK{id: "default"}, crypto.WithKeyConfig("ns1", crypto.KeyConfig{Duration: time.Hour}))

	t.Run("nil message", func(t *testing.T) {
		t.Parallel()
		_, err := v.Open(ctx, nil)
		require.Error(t, err)
	})

	t.Run("nil key material", func(t *testing.T) {
		t.Parallel()
		_, err := v.Open(ctx, &crypto.Message{Ciphertext: []byte("whatever")})
		require.Error(t, err)
	})

	t.Run("unknown kek id", func(t *testing.T) {
		t.Parallel()
		msg := &crypto.Message{
			Ciphertext:  []byte("whatever"),
			KeyMaterial: &crypto.DEKMaterial{KEKID: "missing", EncryptedDEK: "AAAA"},
		}
		_, err := v.Open(ctx, msg)
		require.Error(t, err)
	})

	t.Run("tampered ciphertext", func(t *testing.T) {
		t.Parallel()
		msg, err := v.Seal(ctx, "ns1", []byte("secret"))
		require.NoError(t, err)

		msg.Ciphertext[len(msg.Ciphertext)-1] ^= 0xFF
		_, err = v.Open(ctx, msg)
		require.ErrorIs(t, err, crypto.ErrMalformedCipherText)
	})
}

func TestNamespacedVault(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	v := newVault(t, &countingKEK{id: "default"}, crypto.WithKeyConfig("ns1", crypto.KeyConfig{Duration: time.Hour}))
	nv := v.ForNamespace("ns1")

	msg, err := nv.Seal(ctx, []byte("scoped"))
	require.NoError(t, err)

	pt, err := nv.Open(ctx, msg)
	require.NoError(t, err)
	require.Equal(t, []byte("scoped"), pt)
}

func TestVaultSealCoalescesConcurrentCalls(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		const callers = 8
		kek := &countingKEK{id: "default", block: make(chan struct{})}
		v := newVault(t, kek, crypto.WithKeyConfig("ns1", crypto.KeyConfig{Duration: time.Hour}))

		msgs := make([]*crypto.Message, callers)
		errs := make([]error, callers)

		var wg sync.WaitGroup
		for i := range callers {
			wg.Go(func() {
				msgs[i], errs[i] = v.Seal(context.Background(), "ns1", []byte("payload"))
			})
		}

		// Wait until every goroutine is durably blocked: the singleflight leader
		// inside the KEK call, the rest waiting on it. Then release the leader.
		synctest.Wait()
		close(kek.block)
		wg.Wait()

		// The concurrent first seals collapsed into a single KEK call and all
		// callers received identical material.
		require.Equal(t, int64(1), kek.encCount.Load())
		for i := range callers {
			require.NoError(t, errs[i])
			require.Equal(t, msgs[0].KeyMaterial, msgs[i].KeyMaterial)
		}
	})
}

func TestVaultSealEncryptError(t *testing.T) {
	t.Parallel()

	// A failure wrapping the DEK (e.g. KMS unavailable) propagates out of Seal.
	kek := &countingKEK{id: "default", encErr: errors.New("kms unavailable")}
	v := newVault(t, kek, crypto.WithKeyConfig("ns1", crypto.KeyConfig{Duration: time.Hour}))

	_, err := v.Seal(t.Context(), "ns1", []byte("data"))
	require.Error(t, err)
}

func TestVaultSealMidFlightRotation(t *testing.T) {
	t.Parallel()

	// A seal that snapshots DEK1 and is still wrapping it when Refresh rotates
	// the namespace to DEK2 must not write its material into DEK2's slot. Its own
	// envelope stays self-consistent, and DEK2 is wrapped independently later.
	synctest.Test(t, func(t *testing.T) {
		clock := &testClock{}
		kek := &countingKEK{id: "default", block: make(chan struct{})}
		v := newVault(
			t, kek,
			crypto.WithNowFunc(clock.Now),
			crypto.WithKeyConfig("ns1", crypto.KeyConfig{Duration: time.Hour}),
		)

		var (
			msg     *crypto.Message
			sealErr error
			wg      sync.WaitGroup
		)

		wg.Go(func() {
			msg, sealErr = v.Seal(context.Background(), "ns1", []byte("in-flight"))
		})

		// The caller is durably blocked wrapping DEK1. Expire and rotate the
		// namespace to DEK2 underneath it, then let the wrap finish.
		synctest.Wait()
		clock.advance(90 * time.Minute)
		require.NoError(t, v.Refresh())
		close(kek.block)
		wg.Wait()
		require.NoError(t, sealErr)

		// The in-flight message carries a self-consistent DEK1 envelope.
		pt, err := v.Open(context.Background(), msg)
		require.NoError(t, err)
		require.Equal(t, []byte("in-flight"), pt)

		// A fresh seal uses the rotated DEK2: distinct material and its own KEK
		// wrap, proving DEK2's material slot was left empty by the in-flight call.
		next, err := v.Seal(context.Background(), "ns1", []byte("after"))
		require.NoError(t, err)
		require.NotEqual(t, msg.KeyMaterial.EncryptedDEK, next.KeyMaterial.EncryptedDEK)
		require.Equal(t, int64(2), kek.encCount.Load())

		pt, err = v.Open(context.Background(), next)
		require.NoError(t, err)
		require.Equal(t, []byte("after"), pt)
	})
}

func newVault(t *testing.T, kek crypto.KEK, opts ...crypto.VaultOption) *crypto.Vault {
	t.Helper()
	v, err := crypto.NewVault(newRegistry(t, kek), opts...)
	require.NoError(t, err)
	return v
}

func newRegistry(t *testing.T, extra ...crypto.KEK) *crypto.KEKRegistry {
	t.Helper()
	var def crypto.KEK = &countingKEK{id: "default"}
	if len(extra) > 0 {
		def = extra[0]
	}

	reg, err := crypto.NewKEKRegistry(crypto.WithDefaultKey(def))
	require.NoError(t, err)
	return reg
}

func (k *countingKEK) ID() string { return k.id }

func (k *countingKEK) Encrypt(_ context.Context, pt []byte) ([]byte, error) {
	k.encCount.Add(1)
	if k.block != nil {
		<-k.block
	}
	if k.encErr != nil {
		return nil, k.encErr
	}

	return pt, nil
}

func (k *countingKEK) Decrypt(_ context.Context, ct []byte) ([]byte, error) {
	k.decCount.Add(1)
	return ct, nil
}

func (k *countingKEK) Close() error { return nil }

func (c *testClock) Now() time.Time          { return time.Unix(0, c.nanos.Load()) }
func (c *testClock) advance(d time.Duration) { c.nanos.Add(int64(d)) }
