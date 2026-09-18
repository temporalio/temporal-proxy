package ext_test

import (
	"bytes"
	"context"
	"crypto/cipher"
	"encoding/hex"
	"math"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	extv1 "github.com/temporalio/temporal-proxy/pkg/api/ext/v1"
	"github.com/temporalio/temporal-proxy/pkg/ext"
)

// wrappingKey is a fixed 32-byte key, the size every built-in cipher takes.
var wrappingKey = bytes.Repeat([]byte{0x2a}, 32)

// stubKeys is a key store over one fixed key. sealing is the version it reports
// as current, and it answers for every version it is asked about, the way a key
// store that has forgotten nothing would.
type stubKeys struct {
	key     []byte
	sealing string
	err     error

	mu    sync.Mutex
	asked []ext.KeyRequest
}

func newStubKeys(key []byte) *stubKeys {
	return &stubKeys{key: key, sealing: "1"}
}

// staticKey is a lookup over one key that reports version "1" as current.
func staticKey(key []byte) ext.KeyLookup {
	return newStubKeys(key).lookup
}

func (s *stubKeys) lookup(_ context.Context, req ext.KeyRequest) (ext.Key, error) {
	if s.err != nil {
		return ext.Key{}, s.err
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.asked = append(s.asked, req)

	// An empty request version asks for whatever is current, which is the only
	// point at which this store gets to choose.
	version := req.Version
	if version == "" {
		version = s.sealing
	}

	return ext.Key{Bytes: s.key, Version: version}, nil
}

// requests reports what the wrapper asked for, in order.
func (s *stubKeys) requests() []ext.KeyRequest {
	s.mu.Lock()
	defer s.mu.Unlock()

	return slices.Clone(s.asked)
}

func TestKeyWrapperRoundTrip(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		name  string
		id    ext.CipherID
		nonce int
	}{
		{"aes-256-gcm", ext.CipherAES256GCM, 12},
		{"chacha20-poly1305", ext.CipherChaCha20Poly1305, 12},
		{"xchacha20-poly1305", ext.CipherXChaCha20Poly1305, 24},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			w, err := ext.NewKeyWrapper(staticKey(wrappingKey), ext.WithCipher(tt.id))
			require.NoError(t, err)

			dek := bytes.Repeat([]byte{0x01}, 32)
			sealed, err := w.Wrap(t.Context(), "orders", dek)
			require.NoError(t, err)

			km, err := extv1.UnmarshalKeyMaterial(sealed)
			require.NoError(t, err)
			require.Equal(t, tt.id, km.GetCipher(), "the cipher travels with the material")
			require.Equal(t, "1", km.GetVersion())
			require.Equal(t, "orders", km.GetNamespace())
			require.Len(t, km.GetNonce(), tt.nonce)
			require.Empty(t, km.GetOpaque(), "opaque stays the server's")
			require.NotContains(t, string(km.GetEncryptedDek()), string(dek))

			got, err := w.Unwrap(t.Context(), sealed)
			require.NoError(t, err)
			require.Equal(t, dek, got)
		})
	}
}

func TestKeyWrapperAuthenticatesClearFields(t *testing.T) {
	t.Parallel()

	// Every case here keeps the same wrapping key, so a relabelled field cannot be
	// caught by deriving a different key. The additional data is the only thing
	// standing between these and material that opens under the wrong assumption.
	for _, tt := range []struct {
		name   string
		tamper func(*extv1.KeyMaterial)
	}{
		{"namespace", func(km *extv1.KeyMaterial) { km.Namespace = "payments" }},
		{"version", func(km *extv1.KeyMaterial) { km.Version = "2" }},
		{"cipher", func(km *extv1.KeyMaterial) { km.Cipher = ext.CipherChaCha20Poly1305 }},
		{"opaque", func(km *extv1.KeyMaterial) { km.Opaque = []byte("smuggled") }},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			w, err := ext.NewKeyWrapper(staticKey(wrappingKey))
			require.NoError(t, err)

			sealed, err := w.Wrap(t.Context(), "orders", bytes.Repeat([]byte{0x01}, 32))
			require.NoError(t, err)

			km, err := extv1.UnmarshalKeyMaterial(sealed)
			require.NoError(t, err)
			tt.tamper(km)

			relabelled, err := km.Marshal()
			require.NoError(t, err)

			_, err = w.Unwrap(t.Context(), relabelled)
			require.Error(t, err, "relabelled material must not open")
			require.Equal(t, codes.InvalidArgument, status.Code(err))
		})
	}
}

func TestKeyWrapperOpensMaterialSealedByAnotherCipher(t *testing.T) {
	t.Parallel()

	dek := bytes.Repeat([]byte{0x01}, 32)

	// A server that starts on the default.
	before, err := ext.NewKeyWrapper(staticKey(wrappingKey))
	require.NoError(t, err)

	old, err := before.Wrap(t.Context(), "orders", dek)
	require.NoError(t, err)

	// The same server after its operator switched ciphers. Nothing else changed:
	// same key, same version.
	after, err := ext.NewKeyWrapper(
		staticKey(wrappingKey),
		ext.WithCipher(ext.CipherXChaCha20Poly1305),
	)
	require.NoError(t, err)

	// Everything already sealed still opens, because the material names the cipher
	// that sealed it rather than the one now configured.
	got, err := after.Unwrap(t.Context(), old)
	require.NoError(t, err)
	require.Equal(t, dek, got)

	// New material uses the new cipher, and the old server can no longer read it.
	fresh, err := after.Wrap(t.Context(), "orders", dek)
	require.NoError(t, err)

	km, err := extv1.UnmarshalKeyMaterial(fresh)
	require.NoError(t, err)
	require.Equal(t, ext.CipherXChaCha20Poly1305, km.GetCipher())

	got, err = after.Unwrap(t.Context(), fresh)
	require.NoError(t, err)
	require.Equal(t, dek, got)
}

func TestKeyWrapperUnwrapRejects(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		name     string
		material func(t *testing.T, sealed []byte) []byte
		msg      string
	}{
		{
			name: "material that is not a KeyMaterial at all",
			material: func(*testing.T, []byte) []byte {
				return []byte{0xff, 0xff, 0xff}
			},
			msg: "failed to unmarshal key material",
		},
		{
			name: "material carrying no wrapped DEK",
			material: func(*testing.T, []byte) []byte {
				return nil
			},
			msg: "invalid key material",
		},
		{
			name: "material naming no cipher",
			material: func(t *testing.T, sealed []byte) []byte {
				return remarshal(t, sealed, func(km *extv1.KeyMaterial) {
					km.Cipher = extv1.KeyMaterial_CIPHER_UNSPECIFIED
				})
			},
			msg: "names no cipher",
		},
		{
			name: "material naming a cipher nobody registered",
			material: func(t *testing.T, sealed []byte) []byte {
				return remarshal(t, sealed, func(km *extv1.KeyMaterial) {
					km.Cipher = extv1.KeyMaterial_Cipher(99)
				})
			},
			msg: "no cipher registered for",
		},
		{
			name: "a nonce the cipher cannot use",
			material: func(t *testing.T, sealed []byte) []byte {
				return remarshal(t, sealed, func(km *extv1.KeyMaterial) {
					km.Nonce = km.GetNonce()[:4]
				})
			},
			msg: "carries a 4-byte nonce, want 12",
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			w, err := ext.NewKeyWrapper(staticKey(wrappingKey))
			require.NoError(t, err)

			sealed, err := w.Wrap(t.Context(), "orders", bytes.Repeat([]byte{0x01}, 32))
			require.NoError(t, err)

			_, err = w.Unwrap(t.Context(), tt.material(t, sealed))
			require.Error(t, err)
			require.Contains(t, err.Error(), tt.msg)
		})
	}
}

// remarshal opens sealed material, applies tamper, and packs it again.
func remarshal(t *testing.T, sealed []byte, tamper func(*extv1.KeyMaterial)) []byte {
	t.Helper()

	km, err := extv1.UnmarshalKeyMaterial(sealed)
	require.NoError(t, err)
	tamper(km)

	out, err := km.Marshal()
	require.NoError(t, err)

	return out
}

func TestKeyWrapperCustomCipher(t *testing.T) {
	t.Parallel()

	// Ids from 128 up are reserved for exactly this, so registering one cannot
	// collide with a cipher the proxy adds later.
	const vendor = extv1.KeyMaterial_Cipher(128)

	var called int
	w, err := ext.NewKeyWrapper(
		staticKey(wrappingKey),
		ext.WithCipherFunc(vendor, func(key []byte) (cipher.AEAD, error) {
			called++

			return ext.NewXChaCha20Poly1305(key)
		}),
		ext.WithCipher(vendor),
	)
	require.NoError(t, err)

	dek := bytes.Repeat([]byte{0x01}, 32)
	sealed, err := w.Wrap(t.Context(), "orders", dek)
	require.NoError(t, err)

	km, err := extv1.UnmarshalKeyMaterial(sealed)
	require.NoError(t, err)
	require.Equal(t, vendor, km.GetCipher(), "a custom id round-trips rather than being dropped")

	got, err := w.Unwrap(t.Context(), sealed)
	require.NoError(t, err)
	require.Equal(t, dek, got)
	require.Equal(t, 2, called, "the registered func built the AEAD on both paths")
}

func TestKeyWrapperReportsLookupFailure(t *testing.T) {
	t.Parallel()

	t.Run("keeps the code the lookup chose", func(t *testing.T) {
		t.Parallel()

		down := newStubKeys(wrappingKey)
		down.err = status.Error(codes.Unavailable, "key store is down")

		w, err := ext.NewKeyWrapper(down.lookup)
		require.NoError(t, err)

		_, err = w.Wrap(t.Context(), "orders", bytes.Repeat([]byte{0x01}, 32))
		require.Error(t, err)

		// A retry might help here, and only the lookup knows that, so its code has
		// to survive being wrapped on the way out.
		require.Equal(t, codes.Unavailable, status.Code(err))
	})

	t.Run("names the cipher when the key is the wrong size", func(t *testing.T) {
		t.Parallel()

		w, err := ext.NewKeyWrapper(staticKey(bytes.Repeat([]byte{0x2a}, 16)))
		require.NoError(t, err)

		_, err = w.Wrap(t.Context(), "orders", bytes.Repeat([]byte{0x01}, 32))
		require.ErrorContains(t, err, "AES-256-GCM needs a 32-byte key, got 16")
	})
}

func TestNewKeyWrapperRejects(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		name   string
		lookup ext.KeyLookup
		opts   []ext.KeyWrapperOption
		msg    string
	}{
		{
			name: "no lookup",
			msg:  "a key lookup is required",
		},
		{
			name:   "a cipher nobody registered",
			lookup: staticKey(wrappingKey),
			opts:   []ext.KeyWrapperOption{ext.WithCipher(extv1.KeyMaterial_Cipher(99))},
			msg:    "no cipher registered for",
		},
		{
			name:   "a nil cipher func",
			lookup: staticKey(wrappingKey),
			opts:   []ext.KeyWrapperOption{ext.WithCipherFunc(extv1.KeyMaterial_Cipher(128), nil)},
			msg:    "must not be nil",
		},
		{
			name:   "a cipher id that names nothing",
			lookup: staticKey(wrappingKey),
			opts: []ext.KeyWrapperOption{
				ext.WithCipherFunc(extv1.KeyMaterial_CIPHER_UNSPECIFIED, ext.NewAES256GCM),
			},
			msg: "cipher id must be positive",
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := ext.NewKeyWrapper(tt.lookup, tt.opts...)
			require.ErrorContains(t, err, tt.msg)
		})
	}
}

func TestKeyWrapperFollowsTheKeyStore(t *testing.T) {
	t.Parallel()

	keys := newStubKeys(wrappingKey)
	w, err := ext.NewKeyWrapper(keys.lookup)
	require.NoError(t, err)

	dek := bytes.Repeat([]byte{0x01}, 32)
	old, err := w.Wrap(t.Context(), "orders", dek)
	require.NoError(t, err)

	km, err := extv1.UnmarshalKeyMaterial(old)
	require.NoError(t, err)
	require.Equal(t, "1", km.GetVersion(), "the version the lookup reported is the one stamped")

	// The key store rotates on its own schedule. The wrapper is not reconfigured
	// and this server is not redeployed, which is the whole point of the version
	// being something the lookup reports rather than something it is told.
	keys.sealing = "2"

	fresh, err := w.Wrap(t.Context(), "orders", dek)
	require.NoError(t, err)

	km, err = extv1.UnmarshalKeyMaterial(fresh)
	require.NoError(t, err)
	require.Equal(t, "2", km.GetVersion(), "new material follows the key store")

	// Material sealed before the rotation opens by asking for its own version,
	// not the one that is now current.
	got, err := w.Unwrap(t.Context(), old)
	require.NoError(t, err)
	require.Equal(t, dek, got)

	// Sealing never names a version, and opening always does. That asymmetry is
	// the whole point: the key store decides what is current, and the material
	// decides what is being opened.
	require.Equal(t, []ext.KeyRequest{
		{Namespace: "orders"},
		{Namespace: "orders"},
		{Namespace: "orders", Version: "1"},
	}, keys.requests())
}

func TestKeyWrapperRejectsAnUnframeableVersion(t *testing.T) {
	t.Parallel()

	keys := newStubKeys(wrappingKey)
	keys.sealing = strings.Repeat("v", math.MaxUint16+1)

	w, err := ext.NewKeyWrapper(keys.lookup)
	require.NoError(t, err)

	// Reported at seal time rather than at construction, since only the lookup
	// knows what it is going to say.
	_, err = w.Wrap(t.Context(), "orders", bytes.Repeat([]byte{0x01}, 32))
	require.ErrorContains(t, err, "key version is too long")
}

// Material sealed by this package before [ext.BindingContext] was factored out
// of the additional-data encoding. Every payload the proxy has already sealed
// authenticates against those bytes, so the encoding can never change, and
// these two open only if it has not.
//
// They cannot be regenerated. Regenerating them is the mistake this test
// exists to catch, and the nonce baked into each one means a fresh Wrap will
// not reproduce them by accident either.
const (
	// goldenVersioned carries a version and a namespace.
	goldenVersioned = "0a30a523674f5e170acc69a73940909445859cba48f0d40e5bda0276721656452133f9878d71b20f0" +
		"7050dae9dbdbcd68dcf1201311a066f72646572732a0c05f93a254419028c712c2ef83001"

	// goldenBare carries neither, pinning the zero-length prefixes.
	goldenBare = "0a307f92c94725b5e40a8a5b5cf0e8034253e9e58abbf27620d18e9437f6d0f6e7a28d9d73127d5eece" +
		"c5bc885aadc5b4e872a0c6de468cf0e7e70d33dd364433001"
)

// TestKeyWrapperOpensGoldenMaterial is the gate on the additional-data
// encoding. It fails if a single bit of what [ext.BindingContext] and its
// unexported sibling emit has moved, because the AEAD authenticates every one
// of them.
func TestKeyWrapperOpensGoldenMaterial(t *testing.T) {
	t.Parallel()

	dek := bytes.Repeat([]byte{0x01}, 32)

	for _, tt := range []struct {
		name      string
		sealed    string
		namespace string
		version   string
	}{
		{"versioned", goldenVersioned, "orders", "1"},
		{"bare", goldenBare, "", ""},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			sealed, err := hex.DecodeString(tt.sealed)
			require.NoError(t, err)

			km, err := extv1.UnmarshalKeyMaterial(sealed)
			require.NoError(t, err)
			require.Equal(t, tt.namespace, km.GetNamespace())
			require.Equal(t, tt.version, km.GetVersion())

			// The lookup answers with the same key whatever version is asked for,
			// since the golden material names the one it was sealed under.
			w, err := ext.NewKeyWrapper(func(context.Context, ext.KeyRequest) (ext.Key, error) {
				return ext.Key{Bytes: wrappingKey, Version: tt.version}, nil
			})
			require.NoError(t, err)

			got, err := w.Unwrap(t.Context(), sealed)
			require.NoError(t, err)
			require.Equal(t, dek, got)
		})
	}
}
