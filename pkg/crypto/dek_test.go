package crypto_test

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/temporalio/temporal-proxy/pkg/crypto"
)

func TestNewDEK(t *testing.T) {
	t.Parallel()

	t.Run("non-nil", func(t *testing.T) {
		t.Parallel()

		d, err := crypto.NewDEK()
		require.NoError(t, err)
		require.NotNil(t, d)
	})

	t.Run("distinct keys per call", func(t *testing.T) {
		t.Parallel()

		d1, err := crypto.NewDEK()
		require.NoError(t, err)

		d2, err := crypto.NewDEK()
		require.NoError(t, err)

		ct, err := d1.Encrypt(t.Context(), []byte("sshh"))
		require.NoError(t, err)

		_, err = d2.Decrypt(t.Context(), ct)
		require.ErrorIs(t, err, crypto.ErrMalformedCipherText)
	})
}

func TestDEKEncryptDecrypt(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		plaintext []byte
	}{
		{"empty", []byte{}},
		{"ascii", []byte("hello, world")},
		{"binary", []byte{0x00, 0xFF, 0x42, 0x13}},
		{"large", bytes.Repeat([]byte("a"), 64*1024)},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctx := t.Context()
			dek, err := crypto.NewDEK()
			require.NoError(t, err)

			ct, err := dek.Encrypt(ctx, tc.plaintext)
			require.NoError(t, err)

			pt, err := dek.Decrypt(ctx, ct)
			require.NoError(t, err)
			require.True(t, bytes.Equal(tc.plaintext, pt))
		})
	}
}

func TestDEKEncrypt(t *testing.T) {
	t.Parallel()

	t.Run("unique ciphertext per call", func(t *testing.T) {
		t.Parallel()
		ctx := t.Context()
		dek, err := crypto.NewDEK()
		require.NoError(t, err)

		pt := []byte("same plaintext")
		ct1, err := dek.Encrypt(ctx, pt)
		require.NoError(t, err)

		ct2, err := dek.Encrypt(ctx, pt)
		require.NoError(t, err)

		require.NotEqual(t, ct1, ct2)
	})
}

func TestDEKDecrypt(t *testing.T) {
	t.Parallel()

	t.Run("wrong key", func(t *testing.T) {
		t.Parallel()

		ctx := t.Context()
		dek, err := crypto.NewDEK()
		require.NoError(t, err)

		ct, err := dek.Encrypt(ctx, []byte("secret"))
		require.NoError(t, err)

		altDEK, err := crypto.NewDEK()
		require.NoError(t, err)
		_, err = altDEK.Decrypt(ctx, ct)
		require.Error(t, err)
	})

	t.Run("not encrypted with DEK", func(t *testing.T) {
		t.Parallel()

		dek, err := crypto.NewDEK()
		require.NoError(t, err)

		ct, err := dek.Decrypt(t.Context(), []byte("nope"))
		require.ErrorIs(t, err, crypto.ErrMalformedCipherText)
		require.Nil(t, ct)
	})

	t.Run("tampered ciphertext", func(t *testing.T) {
		t.Parallel()

		ctx := t.Context()
		dek, err := crypto.NewDEK()
		require.NoError(t, err)

		ct, err := dek.Encrypt(ctx, []byte("secret"))
		require.NoError(t, err)

		ct[len(ct)-1] ^= 0xFF
		_, err = dek.Decrypt(ctx, ct)
		require.Error(t, err)
	})
}
