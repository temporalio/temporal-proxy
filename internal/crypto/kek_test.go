package crypto_test

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/temporalio/temporal-proxy/internal/crypto"
)

type fakeKEK struct {
	id         string
	closeCount int
	closeErr   error
	encErr     error
	decErr     error
	decResult  []byte // when non-nil, returned from Decrypt instead of ct
}

func TestKEKRegistryEncrypt(t *testing.T) {
	t.Parallel()

	dek, err := crypto.NewDEK()
	require.NoError(t, err)

	tests := []struct {
		name      string
		ns        string
		opts      []crypto.KEKRegistryOption
		wantErr   bool
		wantKEKID string
	}{
		{
			name:      "unknown namespace falls back to nil kek",
			ns:        "missing",
			wantKEKID: "EMPTY_KEK",
		},
		{
			name:      "unknown namespace uses custom default policy",
			ns:        "missing",
			opts:      []crypto.KEKRegistryOption{crypto.WithDefaultPolicy(crypto.KeyPolicy{KEK: &fakeKEK{id: "default"}})},
			wantKEKID: "default",
		},
		{
			name:    "kek encrypt error",
			ns:      "ns1",
			opts:    []crypto.KEKRegistryOption{crypto.WithKeyPolicy("ns1", crypto.KeyPolicy{KEK: &fakeKEK{id: "k1", encErr: errors.New("kms unavailable")}})},
			wantErr: true,
		},
		{
			name:      "success",
			ns:        "ns1",
			opts:      []crypto.KEKRegistryOption{crypto.WithKeyPolicy("ns1", crypto.KeyPolicy{KEK: &fakeKEK{id: "k1"}})},
			wantKEKID: "k1",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			r := crypto.NewKEKRegistry(tc.opts...)
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
			name:     "unknown key id",
			material: &crypto.DEKMaterial{KEKID: "missing"},
			wantErr:  true,
		},
		{
			name:     "invalid base64",
			material: &crypto.DEKMaterial{KEKID: "k1", EncryptedDEK: "not-valid-base64!!!"},
			opts:     []crypto.KEKRegistryOption{crypto.WithKeyPolicy("ns1", crypto.KeyPolicy{KEK: &fakeKEK{id: "k1"}})},
			wantErr:  true,
		},
		{
			name:     "kek decrypt error",
			material: &crypto.DEKMaterial{KEKID: "k1", EncryptedDEK: "AAAA"},
			opts:     []crypto.KEKRegistryOption{crypto.WithKeyPolicy("ns1", crypto.KeyPolicy{KEK: &fakeKEK{id: "k1", decErr: errors.New("kms unavailable")}})},
			wantErr:  true,
		},
		{
			name:     "wrong-length dek",
			material: &crypto.DEKMaterial{KEKID: "k1", EncryptedDEK: "AAAA"},
			opts:     []crypto.KEKRegistryOption{crypto.WithKeyPolicy("ns1", crypto.KeyPolicy{KEK: &fakeKEK{id: "k1", decResult: []byte("too short")}})},
			wantErr:  true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			r := crypto.NewKEKRegistry(tc.opts...)
			_, err := r.Decrypt(t.Context(), tc.material)
			require.Error(t, err)
		})
	}
}

func TestKEKRegistryRoundtrip(t *testing.T) {
	t.Parallel()

	original, err := crypto.NewDEK()
	require.NoError(t, err)

	r := crypto.NewKEKRegistry(crypto.WithKeyPolicy("ns1", crypto.KeyPolicy{KEK: &fakeKEK{id: "k1"}}))

	m, err := r.Encrypt(t.Context(), "ns1", original)
	require.NoError(t, err)

	recovered, err := r.Decrypt(t.Context(), m)
	require.NoError(t, err)
	require.Equal(t, original, recovered)
}

func TestKEKRegistryClose(t *testing.T) {
	t.Parallel()

	t.Run("closes all keks", func(t *testing.T) {
		t.Parallel()
		k1 := &fakeKEK{id: "k1"}
		k2 := &fakeKEK{id: "k2"}
		r := crypto.NewKEKRegistry(
			crypto.WithKeyPolicy("ns1", crypto.KeyPolicy{KEK: k1}),
			crypto.WithKeyPolicy("ns2", crypto.KeyPolicy{KEK: k2}),
		)

		require.NoError(t, r.Close())
		require.Equal(t, 1, k1.closeCount)
		require.Equal(t, 1, k2.closeCount)
	})

	t.Run("closes default kek", func(t *testing.T) {
		t.Parallel()
		k := &fakeKEK{id: "default"}
		r := crypto.NewKEKRegistry(crypto.WithDefaultPolicy(crypto.KeyPolicy{KEK: k}))

		require.NoError(t, r.Close())
		require.Equal(t, 1, k.closeCount)
	})

	t.Run("returns error on close failure", func(t *testing.T) {
		t.Parallel()
		kek := &fakeKEK{id: "k1", closeErr: errors.New("close failed")}
		r := crypto.NewKEKRegistry(crypto.WithKeyPolicy("ns1", crypto.KeyPolicy{KEK: kek}))

		require.Error(t, r.Close())
	})

	t.Run("idempotent - same result on repeated close", func(t *testing.T) {
		t.Parallel()
		kek := &fakeKEK{id: "k1", closeErr: errors.New("close failed")}
		r := crypto.NewKEKRegistry(crypto.WithKeyPolicy("ns1", crypto.KeyPolicy{KEK: kek}))

		err1 := r.Close()
		err2 := r.Close()
		require.Equal(t, err1, err2)
		require.Equal(t, 1, kek.closeCount)
	})

	t.Run("concurrent close calls kek once", func(t *testing.T) {
		t.Parallel()
		kek := &fakeKEK{id: "k1"}
		r := crypto.NewKEKRegistry(crypto.WithKeyPolicy("ns1", crypto.KeyPolicy{KEK: kek}))

		errs := make([]error, 20)
		var wg sync.WaitGroup
		wg.Add(len(errs))
		for i := range len(errs) {
			go func() {
				defer wg.Done()
				errs[i] = r.Close()
			}()
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
	if f.decResult != nil || f.decErr != nil {
		return f.decResult, f.decErr
	}

	return ct, nil
}

func (f *fakeKEK) Close() error {
	f.closeCount++
	return f.closeErr
}
