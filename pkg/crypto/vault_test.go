package crypto_test

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"

	"go.temporal.io/proxy/pkg/crypto"
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
			name:    "unknown binding mode",
			opts:    []crypto.VaultOption{crypto.WithBindingMode(crypto.BindingMode(99))},
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

// TestVaultOpenUnknownKey shows the sentinel survives Vault.Open, which is what
// the codec actually calls. Wrapping the registry error without %w anywhere in
// Open or open fails it.
func TestVaultOpenUnknownKey(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	cfg := crypto.WithKeyConfig("ns1", crypto.KeyConfig{Duration: time.Hour})
	sealer := newVault(t, &countingKEK{id: "sealer"}, cfg)
	opener := newVault(t, &countingKEK{id: "opener"}, cfg)

	msg, err := sealer.Seal(ctx, "ns1", []byte("data"))
	require.NoError(t, err)

	_, err = opener.Open(ctx, msg)
	require.ErrorIs(t, err, crypto.ErrUnknownKey)
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

// TestVaultBindingModes pins what each mode seals and opens. Making off stop
// opening bound messages, or strict stop rejecting unbound ones, fails a row.
func TestVaultBindingModes(t *testing.T) {
	t.Parallel()

	cfg := crypto.WithKeyConfig("ns1", crypto.KeyConfig{Duration: time.Hour})

	tests := []struct {
		name           string
		mode           crypto.BindingMode
		wantBound      bool
		wantUnboundErr error
	}{
		{name: "off", mode: crypto.BindingOff},
		{name: "permissive", mode: crypto.BindingPermissive, wantBound: true},
		{name: "strict", mode: crypto.BindingStrict, wantBound: true, wantUnboundErr: crypto.ErrUnbound},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			// The identity KEK shares its id across vaults, so each can open what
			// the others sealed.
			bound, err := newVault(t, &countingKEK{id: "default"}, cfg,
				crypto.WithBindingMode(crypto.BindingPermissive)).Seal(ctx, "ns1", []byte("bound"))
			require.NoError(t, err)
			unbound, err := newVault(t, &countingKEK{id: "default"}, cfg).Seal(ctx, "ns1", []byte("unbound"))
			require.NoError(t, err)

			v := newVault(t, &countingKEK{id: "default"}, cfg, crypto.WithBindingMode(tc.mode))

			own, err := v.Seal(ctx, "ns1", []byte("own"))
			require.NoError(t, err)
			require.Equal(t, tc.wantBound, own.Bound)
			if tc.wantBound {
				require.Equal(t, "ns1", own.Namespace)
			} else {
				require.Empty(t, own.Namespace)
			}

			pt, err := v.Open(ctx, bound)
			require.NoError(t, err, "every mode opens bound messages")
			require.Equal(t, []byte("bound"), pt)

			pt, err = v.Open(ctx, unbound)
			if tc.wantUnboundErr != nil {
				require.ErrorIs(t, err, tc.wantUnboundErr)
				return
			}

			require.NoError(t, err)
			require.Equal(t, []byte("unbound"), pt)
		})
	}
}

// TestVaultBindingTamper pins that the binding is authenticated. Dropping the
// AAD from gcm.Seal or gcm.Open makes the relabel rows open; removing the
// version prefix or separator makes the empty-namespace rows open.
func TestVaultBindingTamper(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	cfg := crypto.WithDefaultKeyConfig(crypto.KeyConfig{Duration: time.Hour})
	permissive := newVault(t, &countingKEK{id: "default"}, cfg, crypto.WithBindingMode(crypto.BindingPermissive))
	off := newVault(t, &countingKEK{id: "default"}, cfg)

	seal := func(v *crypto.Vault, ns string) *crypto.Message {
		msg, err := v.Seal(ctx, ns, []byte("secret"))
		require.NoError(t, err)
		return msg
	}

	tests := []struct {
		name   string
		msg    *crypto.Message
		tamper func(*crypto.Message)
	}{
		{name: "relabel", msg: seal(permissive, "ns1"), tamper: func(m *crypto.Message) { m.Namespace = "ns2" }},
		{name: "strip", msg: seal(permissive, "ns1"), tamper: func(m *crypto.Message) { m.Bound = false; m.Namespace = "" }},
		{name: "add to legacy", msg: seal(off, "ns1"), tamper: func(m *crypto.Message) { m.Bound = true; m.Namespace = "ns1" }},
		{name: "strip empty namespace", msg: seal(permissive, ""), tamper: func(m *crypto.Message) { m.Bound = false }},
		{name: "add empty namespace to legacy", msg: seal(off, ""), tamper: func(m *crypto.Message) { m.Bound = true }},
		{name: "non-utf8 label", msg: seal(permissive, "ns1"), tamper: func(m *crypto.Message) { m.Namespace = "\xff\x00\xfe" }},
		{name: "oversized label", msg: seal(permissive, "ns1"), tamper: func(m *crypto.Message) {
			m.Namespace = string(bytes.Repeat([]byte("n"), 64*1024))
		}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			tc.tamper(tc.msg)
			_, err := permissive.Open(ctx, tc.msg)
			require.ErrorIs(t, err, crypto.ErrMalformedCipherText)
		})
	}
}

// TestVaultStrictRejectsBeforeUnwrap pins where the strict check sits. Moving
// it after the unwrap makes decCount non-zero; moving it before the KEK lookup
// turns the foreign row into ErrUnbound; moving it after the cache lookup makes
// the cached row emit a CacheEvent for the refused message.
func TestVaultStrictRejectsBeforeUnwrap(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	cfg := crypto.WithKeyConfig("ns1", crypto.KeyConfig{Duration: time.Hour})

	legacy, err := newVault(t, &countingKEK{id: "default"}, cfg).Seal(ctx, "ns1", []byte("legacy"))
	require.NoError(t, err)

	t.Run("held key", func(t *testing.T) {
		t.Parallel()

		kek := &countingKEK{id: "default"}
		v := newVault(t, kek, cfg, crypto.WithBindingMode(crypto.BindingStrict))

		_, err := v.Open(ctx, legacy)
		require.ErrorIs(t, err, crypto.ErrUnbound)
		require.Zero(t, kek.decCount.Load(), "a refused message must not reach the KEK")
	})

	t.Run("foreign key", func(t *testing.T) {
		t.Parallel()

		kek := &countingKEK{id: "default"}
		v := newVault(t, kek, cfg, crypto.WithBindingMode(crypto.BindingStrict))

		foreign := *legacy
		foreign.KeyMaterial = &crypto.DEKMaterial{KEKID: "other-proxy", EncryptedDEK: legacy.KeyMaterial.EncryptedDEK}

		_, err := v.Open(ctx, &foreign)
		require.ErrorIs(t, err, crypto.ErrUnknownKey)
		require.NotErrorIs(t, err, crypto.ErrUnbound)
		require.Zero(t, kek.decCount.Load())
	})

	t.Run("cached DEK", func(t *testing.T) {
		t.Parallel()

		spy := &spyObserver{}
		v := newVault(t, &countingKEK{id: "default"}, cfg,
			crypto.WithBindingMode(crypto.BindingStrict), crypto.WithObserver(spy))

		bound, err := v.Seal(ctx, "ns1", []byte("bound"))
		require.NoError(t, err)
		_, err = v.Open(ctx, bound) // caches the DEK under bound's material
		require.NoError(t, err)

		stripped := *bound
		stripped.Bound = false
		stripped.Namespace = ""

		spy.reset()
		_, err = v.Open(ctx, &stripped)
		require.ErrorIs(t, err, crypto.ErrUnbound)
		require.Empty(t, spy.cacheHitSizes(), "a refused message must not read the cache")
		require.Empty(t, spy.cacheMissSizes(), "a refused message must not read the cache")
		require.Len(t, spy.envelopeOps(), 1)
	})
}

// TestVaultSealWithLabel pins that the label is independent of the namespace
// that selects the key. Labeling with ns instead of the option fails the bound
// rows; selecting the key by label fails every row, since only ns1 has a key.
func TestVaultSealWithLabel(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		mode      crypto.BindingMode
		opts      []crypto.SealOption
		wantBound bool
		wantNS    string
	}{
		{name: "label given", mode: crypto.BindingPermissive, opts: []crypto.SealOption{crypto.WithLabel("ns1.acct")}, wantBound: true, wantNS: "ns1.acct"},
		{name: "empty label given", mode: crypto.BindingPermissive, opts: []crypto.SealOption{crypto.WithLabel("")}, wantBound: true, wantNS: ""},
		{name: "no label defaults to ns", mode: crypto.BindingPermissive, wantBound: true, wantNS: "ns1"},
		{name: "off ignores the label", mode: crypto.BindingOff, opts: []crypto.SealOption{crypto.WithLabel("ns1.acct")}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			v := newVault(t, &countingKEK{id: "default"},
				crypto.WithKeyConfig("ns1", crypto.KeyConfig{Duration: time.Hour}),
				crypto.WithBindingMode(tc.mode))

			msg, err := v.Seal(t.Context(), "ns1", []byte("x"), tc.opts...)
			require.NoError(t, err)
			require.Equal(t, tc.wantBound, msg.Bound)
			require.Equal(t, tc.wantNS, msg.Namespace)

			pt, err := v.Open(t.Context(), msg)
			require.NoError(t, err)
			require.Equal(t, []byte("x"), pt)
		})
	}
}

// TestVaultOpenExpectNamespace pins which modes enforce the expected namespace.
// Enforcing outside strict fails the permissive and off mismatch rows; skipping
// the comparison fails the strict mismatch row; comparing without an
// expectation fails the last row.
func TestVaultOpenExpectNamespace(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		mode    crypto.BindingMode
		label   string
		opts    []crypto.OpenOption
		wantErr error
	}{
		{name: "strict match", mode: crypto.BindingStrict, label: "a", opts: []crypto.OpenOption{crypto.ExpectNamespace("a")}},
		{name: "strict match on empty", mode: crypto.BindingStrict, label: "", opts: []crypto.OpenOption{crypto.ExpectNamespace("")}},
		{
			name: "strict mismatch", mode: crypto.BindingStrict, label: "a",
			opts: []crypto.OpenOption{crypto.ExpectNamespace("b")}, wantErr: crypto.ErrNamespaceMismatch,
		},
		{
			name: "strict empty expected, labeled", mode: crypto.BindingStrict, label: "a",
			opts: []crypto.OpenOption{crypto.ExpectNamespace("")}, wantErr: crypto.ErrNamespaceMismatch,
		},
		{name: "permissive mismatch opens", mode: crypto.BindingPermissive, label: "a", opts: []crypto.OpenOption{crypto.ExpectNamespace("b")}},
		{name: "off mismatch opens", mode: crypto.BindingOff, label: "a", opts: []crypto.OpenOption{crypto.ExpectNamespace("b")}},
		{name: "strict without expectation opens", mode: crypto.BindingStrict, label: "a"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			cfg := crypto.WithDefaultKeyConfig(crypto.KeyConfig{Duration: time.Hour})
			sealer := newVault(t, &countingKEK{id: "default"}, cfg, crypto.WithBindingMode(crypto.BindingPermissive))
			msg, err := sealer.Seal(ctx, "ns1", []byte("x"), crypto.WithLabel(tc.label))
			require.NoError(t, err)

			v := newVault(t, &countingKEK{id: "default"}, cfg, crypto.WithBindingMode(tc.mode))
			pt, err := v.Open(ctx, msg, tc.opts...)
			if tc.wantErr != nil {
				require.ErrorIs(t, err, tc.wantErr)
				return
			}

			require.NoError(t, err)
			require.Equal(t, []byte("x"), pt)
		})
	}
}

// TestVaultStrictMismatchBeforeUnwrap pins where the mismatch check sits.
// Moving it after the unwrap makes decCount non-zero; moving it before the KEK
// lookup turns the foreign row into ErrNamespaceMismatch; moving it after the
// cache lookup emits a CacheEvent for the refused message.
func TestVaultStrictMismatchBeforeUnwrap(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	cfg := crypto.WithKeyConfig("ns1", crypto.KeyConfig{Duration: time.Hour})
	sealer := newVault(t, &countingKEK{id: "default"}, cfg, crypto.WithBindingMode(crypto.BindingPermissive))
	msg, err := sealer.Seal(ctx, "ns1", []byte("x"), crypto.WithLabel("a"))
	require.NoError(t, err)

	t.Run("held key", func(t *testing.T) {
		t.Parallel()

		kek := &countingKEK{id: "default"}
		v := newVault(t, kek, cfg, crypto.WithBindingMode(crypto.BindingStrict))

		_, err := v.Open(ctx, msg, crypto.ExpectNamespace("b"))
		require.ErrorIs(t, err, crypto.ErrNamespaceMismatch)
		require.Zero(t, kek.decCount.Load(), "a refused message must not reach the KEK")
	})

	t.Run("foreign key", func(t *testing.T) {
		t.Parallel()

		kek := &countingKEK{id: "default"}
		v := newVault(t, kek, cfg, crypto.WithBindingMode(crypto.BindingStrict))

		foreign := *msg
		foreign.KeyMaterial = &crypto.DEKMaterial{KEKID: "other-proxy", EncryptedDEK: msg.KeyMaterial.EncryptedDEK}

		_, err := v.Open(ctx, &foreign, crypto.ExpectNamespace("b"))
		require.ErrorIs(t, err, crypto.ErrUnknownKey)
		require.NotErrorIs(t, err, crypto.ErrNamespaceMismatch)
		require.Zero(t, kek.decCount.Load())
	})

	t.Run("cached DEK", func(t *testing.T) {
		t.Parallel()

		spy := &spyObserver{}
		v := newVault(t, &countingKEK{id: "default"}, cfg,
			crypto.WithBindingMode(crypto.BindingStrict), crypto.WithObserver(spy))

		_, err := v.Open(ctx, msg, crypto.ExpectNamespace("a")) // caches the DEK
		require.NoError(t, err)
		spy.reset()

		_, err = v.Open(ctx, msg, crypto.ExpectNamespace("b"))
		require.ErrorIs(t, err, crypto.ErrNamespaceMismatch)
		require.Empty(t, spy.cacheHitSizes(), "a refused message must not read the cache")
		require.Empty(t, spy.cacheMissSizes(), "a refused message must not read the cache")
		require.Len(t, spy.envelopeOps(), 1)
	})
}

// Golden namespace-binding material. Never regenerate these: every bound
// payload in every workflow history authenticates against the AAD bytes they
// pin, so if this test fails the AAD encoding moved and must be put back.
// goldenDEK is the DEK (bytes 0x01 through 0x20) and, because countingKEK is
// an identity KEK, also its wrapped form. Both ciphertexts seal "golden" under
// nonce 0xa0 through 0xab: goldenBound for namespace "orders", goldenLegacy
// with no additional data.
const (
	goldenDEK    = "AQIDBAUGBwgJCgsMDQ4PEBESExQVFhcYGRobHB0eHyA="
	goldenBound  = "a0a1a2a3a4a5a6a7a8a9aaabdebe510298db13e25ce2c32e82187f0ee59eb8c747ad"
	goldenLegacy = "a0a1a2a3a4a5a6a7a8a9aaabdebe510298db1b3ab944a2f7b14522cafef884562780"
)

func TestVaultOpensGoldenMaterial(t *testing.T) {
	t.Parallel()

	golden := func(t *testing.T, ct string, bound bool, ns string) *crypto.Message {
		t.Helper()

		raw, err := hex.DecodeString(ct)
		require.NoError(t, err)

		return &crypto.Message{
			Ciphertext:  raw,
			KeyMaterial: &crypto.DEKMaterial{KEKID: "default", EncryptedDEK: goldenDEK},
			Bound:       bound,
			Namespace:   ns,
		}
	}

	tests := []struct {
		name    string
		ct      string
		bound   bool
		mode    crypto.BindingMode
		wantErr error
	}{
		{name: "bound under off", ct: goldenBound, bound: true, mode: crypto.BindingOff},
		{name: "bound under permissive", ct: goldenBound, bound: true, mode: crypto.BindingPermissive},
		{name: "bound under strict", ct: goldenBound, bound: true, mode: crypto.BindingStrict},
		{name: "legacy under off", ct: goldenLegacy, mode: crypto.BindingOff},
		{name: "legacy under permissive", ct: goldenLegacy, mode: crypto.BindingPermissive},
		{name: "legacy under strict", ct: goldenLegacy, mode: crypto.BindingStrict, wantErr: crypto.ErrUnbound},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			ns := ""
			if tc.bound {
				ns = "orders"
			}

			v := newVault(t, &countingKEK{id: "default"}, crypto.WithBindingMode(tc.mode))
			pt, err := v.Open(t.Context(), golden(t, tc.ct, tc.bound, ns))
			if tc.wantErr != nil {
				require.ErrorIs(t, err, tc.wantErr)
				return
			}

			require.NoError(t, err)
			require.Equal(t, []byte("golden"), pt)
		})
	}
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

func (k *countingKEK) Encrypt(_ context.Context, _ string, pt []byte) ([]byte, error) {
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
