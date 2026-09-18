package ext_test

import (
	"bytes"
	"crypto/cipher"
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
