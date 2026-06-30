package crypto_test

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/temporalio/temporal-proxy/pkg/crypto"
)

type fakeKEK struct {
	id         string
	closeCount int
	closeErr   error
	encErr     error
	decErr     error
	plaintext  []byte // when non-nil, returned from Decrypt instead of ct
}

func TestNewKEKRegistry(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		opts    []crypto.KEKRegistryOption
		wantErr bool
	}{
		{
			name:    "requires a default key",
			opts:    []crypto.KEKRegistryOption{crypto.WithKeyForNamespace("ns1", &fakeKEK{id: "k1"})},
			wantErr: true,
		},
		{
			name:    "nil default key",
			opts:    []crypto.KEKRegistryOption{crypto.WithDefaultKey(nil)},
			wantErr: true,
		},
		{
			name: "nil namespace key",
			opts: []crypto.KEKRegistryOption{
				crypto.WithDefaultKey(&fakeKEK{id: "default"}),
				crypto.WithKeyForNamespace("ns1", nil),
			},
			wantErr: true,
		},
		{
			name: "duplicate namespace key",
			opts: []crypto.KEKRegistryOption{
				crypto.WithDefaultKey(&fakeKEK{id: "default"}),
				crypto.WithKeyForNamespace("ns1", &fakeKEK{id: "k1"}),
				crypto.WithKeyForNamespace("ns1", &fakeKEK{id: "k2"}),
			},
			wantErr: true,
		},
		{
			name: "nil decrypt-only key",
			opts: []crypto.KEKRegistryOption{
				crypto.WithDefaultKey(&fakeKEK{id: "default"}),
				crypto.WithDecryptOnlyKey(nil),
			},
			wantErr: true,
		},
		{
			name: "duplicate decrypt-only key",
			opts: func() []crypto.KEKRegistryOption {
				decryptOnly := &fakeKEK{id: "decrypt-only"}
				return []crypto.KEKRegistryOption{
					crypto.WithDefaultKey(&fakeKEK{id: "default"}),
					crypto.WithDecryptOnlyKey(decryptOnly),
					crypto.WithDecryptOnlyKey(decryptOnly),
				}
			}(),
			wantErr: true,
		},
		{
			name: "namespace key shares id with default key",
			opts: []crypto.KEKRegistryOption{
				crypto.WithDefaultKey(&fakeKEK{id: "dup"}),
				crypto.WithKeyForNamespace("ns1", &fakeKEK{id: "dup"}),
			},
			wantErr: true,
		},
		{
			name: "decrypt-only key shares id with default key",
			opts: []crypto.KEKRegistryOption{
				crypto.WithDefaultKey(&fakeKEK{id: "dup"}),
				crypto.WithDecryptOnlyKey(&fakeKEK{id: "dup"}),
			},
			wantErr: true,
		},
		{
			name: "decrypt-only key shares id with namespace key",
			opts: []crypto.KEKRegistryOption{
				crypto.WithDefaultKey(&fakeKEK{id: "default"}),
				crypto.WithKeyForNamespace("ns1", &fakeKEK{id: "dup"}),
				crypto.WithDecryptOnlyKey(&fakeKEK{id: "dup"}),
			},
			wantErr: true,
		},
		{
			name: "distinct namespace keys share id",
			opts: []crypto.KEKRegistryOption{
				crypto.WithDefaultKey(&fakeKEK{id: "default"}),
				crypto.WithKeyForNamespace("ns1", &fakeKEK{id: "dup"}),
				crypto.WithKeyForNamespace("ns2", &fakeKEK{id: "dup"}),
			},
			wantErr: true,
		},
		{
			name: "success",
			opts: []crypto.KEKRegistryOption{
				crypto.WithDefaultKey(&fakeKEK{id: "default"}),
				crypto.WithKeyForNamespace("ns1", &fakeKEK{id: "k1"}),
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			r, err := crypto.NewKEKRegistry(tc.opts...)
			if tc.wantErr {
				require.Error(t, err)
				require.Nil(t, r)
				return
			}

			require.NoError(t, err)
			require.NotNil(t, r)
		})
	}
}

func TestKEKRegistryEncrypt(t *testing.T) {
	t.Parallel()

	dek, err := crypto.NewDEK()
	require.NoError(t, err)

	t.Run("nil dek", func(t *testing.T) {
		t.Parallel()
		r, err := crypto.NewKEKRegistry(crypto.WithDefaultKey(&fakeKEK{id: "default"}))
		require.NoError(t, err)

		_, err = r.Encrypt(t.Context(), "ns1", nil)
		require.Error(t, err)
	})

	tests := []struct {
		name      string
		ns        string
		opts      []crypto.KEKRegistryOption
		wantErr   bool
		wantKEKID string
	}{
		{
			name:      "unknown namespace falls back to default key",
			ns:        "missing",
			opts:      []crypto.KEKRegistryOption{crypto.WithDefaultKey(&fakeKEK{id: "default"})},
			wantKEKID: "default",
		},
		{
			name: "kek encrypt error",
			ns:   "ns1",
			opts: []crypto.KEKRegistryOption{
				crypto.WithDefaultKey(&fakeKEK{id: "default"}),
				crypto.WithKeyForNamespace("ns1", &fakeKEK{id: "k1", encErr: errors.New("kms unavailable")}),
			},
			wantErr: true,
		},
		{
			name: "success",
			ns:   "ns1",
			opts: []crypto.KEKRegistryOption{
				crypto.WithDefaultKey(&fakeKEK{id: "default"}),
				crypto.WithKeyForNamespace("ns1", &fakeKEK{id: "k1"}),
			},
			wantKEKID: "k1",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			r, err := crypto.NewKEKRegistry(tc.opts...)
			require.NoError(t, err)

			m, err := r.Encrypt(t.Context(), tc.ns, dek)
			if tc.wantErr {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)
			require.Equal(t, tc.wantKEKID, m.KEKID)
			require.NotEmpty(t, m.EncryptedDEK)
		})
	}
}

func TestKEKRegistryDecrypt(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		material *crypto.DEKMaterial
		opts     []crypto.KEKRegistryOption
		wantErr  bool
	}{
		{
			name:     "nil material",
			material: nil,
			opts:     []crypto.KEKRegistryOption{crypto.WithDefaultKey(&fakeKEK{id: "default"})},
			wantErr:  true,
		},
		{
			name:     "unknown key id",
			material: &crypto.DEKMaterial{KEKID: "missing"},
			opts:     []crypto.KEKRegistryOption{crypto.WithDefaultKey(&fakeKEK{id: "default"})},
			wantErr:  true,
		},
		{
			name:     "invalid base64",
			material: &crypto.DEKMaterial{KEKID: "k1", EncryptedDEK: "not-valid-base64!!!"},
			opts: []crypto.KEKRegistryOption{
				crypto.WithDefaultKey(&fakeKEK{id: "default"}),
				crypto.WithKeyForNamespace("ns1", &fakeKEK{id: "k1"}),
			},
			wantErr: true,
		},
		{
			name:     "kek decrypt error",
			material: &crypto.DEKMaterial{KEKID: "k1", EncryptedDEK: "AAAA"},
			opts: []crypto.KEKRegistryOption{
				crypto.WithDefaultKey(&fakeKEK{id: "default"}),
				crypto.WithKeyForNamespace("ns1", &fakeKEK{id: "k1", decErr: errors.New("kms unavailable")}),
			},
			wantErr: true,
		},
		{
			name:     "wrong-length dek",
			material: &crypto.DEKMaterial{KEKID: "k1", EncryptedDEK: "AAAA"},
			opts: []crypto.KEKRegistryOption{
				crypto.WithDefaultKey(&fakeKEK{id: "default"}),
				crypto.WithKeyForNamespace("ns1", &fakeKEK{id: "k1", plaintext: []byte("too short")}),
			},
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			r, err := crypto.NewKEKRegistry(tc.opts...)
			require.NoError(t, err)

			_, err = r.Decrypt(t.Context(), tc.material)
			require.Error(t, err)
		})
	}
}

func TestKEKRegistryRoundtrip(t *testing.T) {
	t.Parallel()

	original, err := crypto.NewDEK()
	require.NoError(t, err)

	r, err := crypto.NewKEKRegistry(
		crypto.WithDefaultKey(&fakeKEK{id: "default"}),
		crypto.WithKeyForNamespace("ns1", &fakeKEK{id: "k1"}),
	)
	require.NoError(t, err)

	m, err := r.Encrypt(t.Context(), "ns1", original)
	require.NoError(t, err)

	recovered, err := r.Decrypt(t.Context(), m)
	require.NoError(t, err)
	require.Equal(t, original, recovered)
}

func TestKEKRegistryDefaultKeyRoundtrip(t *testing.T) {
	t.Parallel()

	// A DEK encrypted via the default key must be decryptable by the same registry.
	original, err := crypto.NewDEK()
	require.NoError(t, err)

	defaultKEK := &fakeKEK{id: "default"}
	r, err := crypto.NewKEKRegistry(crypto.WithDefaultKey(defaultKEK))
	require.NoError(t, err)

	m, err := r.Encrypt(t.Context(), "unknown-ns", original)
	require.NoError(t, err)
	require.Equal(t, "default", m.KEKID)

	recovered, err := r.Decrypt(t.Context(), m)
	require.NoError(t, err)
	require.Equal(t, original, recovered)
}

func TestKEKRegistryDecryptOnlyKeyDecrypt(t *testing.T) {
	t.Parallel()

	original, err := crypto.NewDEK()
	require.NoError(t, err)

	active := &fakeKEK{id: "active"}
	decryptOnly := &fakeKEK{id: "decrypt-only"}

	// Encrypt with the decrypt-only key directly (simulating a DEK from before key rotation).
	r, err := crypto.NewKEKRegistry(
		crypto.WithDefaultKey(&fakeKEK{id: "default"}),
		crypto.WithKeyForNamespace("ns1", decryptOnly),
	)
	require.NoError(t, err)

	m, err := r.Encrypt(t.Context(), "ns1", original)
	require.NoError(t, err)
	require.Equal(t, "decrypt-only", m.KEKID)

	// New registry: active key for encryption, the rotated-out key for decryption only.
	r2, err := crypto.NewKEKRegistry(
		crypto.WithDefaultKey(&fakeKEK{id: "default"}),
		crypto.WithKeyForNamespace("ns1", active),
		crypto.WithDecryptOnlyKey(decryptOnly),
	)
	require.NoError(t, err)

	t.Run("new DEKs use active key", func(t *testing.T) {
		t.Parallel()
		m2, err := r2.Encrypt(t.Context(), "ns1", original)
		require.NoError(t, err)
		require.Equal(t, "active", m2.KEKID)
	})

	t.Run("old DEKs decrypt via decrypt-only key", func(t *testing.T) {
		t.Parallel()
		recovered, err := r2.Decrypt(t.Context(), m)
		require.NoError(t, err)
		require.Equal(t, original, recovered)
	})
}

func TestKEKRegistryClose(t *testing.T) {
	t.Parallel()

	t.Run("closes all keks", func(t *testing.T) {
		t.Parallel()
		k1 := &fakeKEK{id: "k1"}
		k2 := &fakeKEK{id: "k2"}
		r, err := crypto.NewKEKRegistry(
			crypto.WithDefaultKey(&fakeKEK{id: "default"}),
			crypto.WithKeyForNamespace("ns1", k1),
			crypto.WithKeyForNamespace("ns2", k2),
		)
		require.NoError(t, err)

		require.NoError(t, r.Close())
		require.Equal(t, 1, k1.closeCount)
		require.Equal(t, 1, k2.closeCount)
	})

	t.Run("returns error on close failure", func(t *testing.T) {
		t.Parallel()
		kek := &fakeKEK{id: "k1", closeErr: errors.New("close failed")}
		r, err := crypto.NewKEKRegistry(
			crypto.WithDefaultKey(&fakeKEK{id: "default"}),
			crypto.WithKeyForNamespace("ns1", kek),
		)
		require.NoError(t, err)

		require.Error(t, r.Close())
	})

	t.Run("idempotent - same result on repeated close", func(t *testing.T) {
		t.Parallel()
		kek := &fakeKEK{id: "k1", closeErr: errors.New("close failed")}
		r, err := crypto.NewKEKRegistry(
			crypto.WithDefaultKey(&fakeKEK{id: "default"}),
			crypto.WithKeyForNamespace("ns1", kek),
		)
		require.NoError(t, err)

		err1 := r.Close()
		err2 := r.Close()
		require.Equal(t, err1, err2)
		require.Equal(t, 1, kek.closeCount)
	})

	t.Run("closes decrypt-only keys", func(t *testing.T) {
		t.Parallel()
		active := &fakeKEK{id: "active"}
		decryptOnly := &fakeKEK{id: "decrypt-only"}
		r, err := crypto.NewKEKRegistry(
			crypto.WithDefaultKey(&fakeKEK{id: "default"}),
			crypto.WithKeyForNamespace("ns1", active),
			crypto.WithDecryptOnlyKey(decryptOnly),
		)
		require.NoError(t, err)

		require.NoError(t, r.Close())
		require.Equal(t, 1, active.closeCount)
		require.Equal(t, 1, decryptOnly.closeCount)
	})

	t.Run("concurrent close calls kek once", func(t *testing.T) {
		t.Parallel()
		kek := &fakeKEK{id: "k1"}
		r, err := crypto.NewKEKRegistry(
			crypto.WithDefaultKey(&fakeKEK{id: "default"}),
			crypto.WithKeyForNamespace("ns1", kek),
		)
		require.NoError(t, err)

		errs := make([]error, 20)
		var wg sync.WaitGroup
		for i := range len(errs) {
			wg.Go(func() {
				errs[i] = r.Close()
			})
		}
		wg.Wait()

		require.Equal(t, 1, kek.closeCount)
		for _, err := range errs {
			require.NoError(t, err)
		}
	})
}

func (f *fakeKEK) ID() string { return f.id }

func (f *fakeKEK) Encrypt(_ context.Context, pt []byte) ([]byte, error) {
	if f.encErr != nil {
		return nil, f.encErr
	}

	return pt, nil
}

func (f *fakeKEK) Decrypt(_ context.Context, ct []byte) ([]byte, error) {
	if f.plaintext != nil || f.decErr != nil {
		return f.plaintext, f.decErr
	}

	return ct, nil
}

func (f *fakeKEK) Close() error {
	f.closeCount++
	return f.closeErr
}
