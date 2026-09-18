package ext_test

import (
	"bytes"
	"context"
	"crypto/cipher"
	"fmt"
	"math"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/temporalio/temporal-proxy/pkg/ext"
)

func TestCiphers(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		name  string
		build ext.CipherFunc
		nonce int
	}{
		{"AES-256-GCM", ext.NewAES256GCM, 12},
		{"ChaCha20-Poly1305", ext.NewChaCha20Poly1305, 12},
		{"XChaCha20-Poly1305", ext.NewXChaCha20Poly1305, 24},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			aead, err := tt.build(bytes.Repeat([]byte{0x2a}, 32))
			require.NoError(t, err)
			require.Equal(t, tt.nonce, aead.NonceSize())

			// Every built-in takes the same key size, so a lookup does not have to
			// know which cipher its key is destined for.
			for _, size := range []int{0, 16, 31, 33} {
				_, err := tt.build(bytes.Repeat([]byte{0x2a}, size))
				require.ErrorContains(t, err, tt.name+" needs a 32-byte key")
			}
		})
	}
}

// TestCipherFuncSignature keeps the built-ins assignable to [ext.CipherFunc], so
// one can be registered under a custom id or replace another.
func TestCipherFuncSignature(t *testing.T) {
	t.Parallel()

	fns := []ext.CipherFunc{
		ext.NewAES256GCM,
		ext.NewChaCha20Poly1305,
		ext.NewXChaCha20Poly1305,
	}

	for _, fn := range fns {
		var aead cipher.AEAD
		aead, err := fn(bytes.Repeat([]byte{0x2a}, 32))
		require.NoError(t, err)
		require.NotNil(t, aead)
	}
}

func TestMustCipherID(t *testing.T) {
	t.Parallel()

	t.Run("accepts the range reserved for custom ciphers", func(t *testing.T) {
		t.Parallel()

		for _, id := range []int{128, 129, 1 << 20, math.MaxInt32} {
			require.EqualValues(t, id, ext.MustCipherID(id))
		}
	})

	t.Run("panics outside it", func(t *testing.T) {
		t.Parallel()

		for _, tt := range []struct {
			name string
			id   int
		}{
			{"unspecified", 0},
			{"negative", -1},
			{"a built-in", int(ext.CipherAES256GCM)},
			{"one below the range", 127},
			// An unbounded conversion would wrap these into an int32: the first to a
			// negative id, the second onto 128, silently colliding with whatever
			// cipher a server registered there.
			{"one past int32", math.MaxInt32 + 1},
			{"wraps onto a valid id", 1<<32 + 128},
		} {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()

				want := fmt.Sprintf("custom cipher id must be between 128 and %d, got %d", math.MaxInt32, tt.id)
				require.PanicsWithValue(t, want, func() { ext.MustCipherID(tt.id) })
			})
		}
	})
}

// TestMustCipherIDRegisters keeps the id usable where it is meant to be used:
// handed straight to [ext.WithCipherFunc] without a conversion at the call site.
func TestMustCipherIDRegisters(t *testing.T) {
	t.Parallel()

	id := ext.MustCipherID(200)

	w, err := ext.NewKeyWrapper(
		func(context.Context, ext.KeyRequest) (ext.Key, error) {
			return ext.Key{Bytes: bytes.Repeat([]byte{0x2a}, 32)}, nil
		},
		ext.WithCipherFunc(id, ext.NewChaCha20Poly1305),
		ext.WithCipher(id),
	)
	require.NoError(t, err)

	dek := bytes.Repeat([]byte{0x01}, 32)
	sealed, err := w.Wrap(t.Context(), "orders", dek)
	require.NoError(t, err)

	got, err := w.Unwrap(t.Context(), sealed)
	require.NoError(t, err)
	require.Equal(t, dek, got)
}
