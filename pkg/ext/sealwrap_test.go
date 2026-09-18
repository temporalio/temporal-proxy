package ext_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"math"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	extv1 "github.com/temporalio/temporal-proxy/pkg/api/ext/v1"
	"github.com/temporalio/temporal-proxy/pkg/ext"
)

// stubSealer is a miniature of a real key service: it holds a key the wrapper
// never sees, and binds what [ext.BindingContext] hands it.
//
// It is deliberately not an identity function. The wrapper enforces no binding
// and cannot, so a sealer that ignored BindingContext would pass every test
// here except the tampering ones, which is exactly the property those tests
// exist to demonstrate.
type stubSealer struct {
	key     []byte
	version string
	opaque  []byte

	mu   sync.Mutex
	seen []ext.OpenRequest
}

// funcSealer covers the cases a working key service cannot produce.
type funcSealer struct {
	seal func(context.Context, ext.SealRequest) (ext.SealedKey, error)
	open func(context.Context, ext.OpenRequest) ([]byte, error)
}

func (s *stubSealer) Seal(_ context.Context, req ext.SealRequest) (ext.SealedKey, error) {
	aead, err := ext.NewAES256GCM(s.key)
	if err != nil {
		return ext.SealedKey{}, err
	}

	ad, err := ext.BindingContext(req.Namespace, s.version, s.opaque)
	if err != nil {
		return ext.SealedKey{}, err
	}

	nonce := make([]byte, aead.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return ext.SealedKey{}, err
	}

	// The nonce rides in front of the ciphertext. A real key service keeps its
	// own framing out of the proxy's sight in exactly this way.
	return ext.SealedKey{
		Ciphertext: aead.Seal(nonce, nonce, req.DEK, ad),
		Version:    s.version,
		Opaque:     s.opaque,
	}, nil
}

func (s *stubSealer) Open(_ context.Context, req ext.OpenRequest) ([]byte, error) {
	s.mu.Lock()
	s.seen = append(s.seen, req)
	s.mu.Unlock()

	aead, err := ext.NewAES256GCM(s.key)
	if err != nil {
		return nil, err
	}

	// Rebuilt from the material, which is what makes a relabelled field fail.
	ad, err := ext.BindingContext(req.Namespace, req.Version, req.Opaque)
	if err != nil {
		return nil, err
	}

	size := aead.NonceSize()
	if len(req.Ciphertext) < size {
		return nil, errors.New("ciphertext is too short to carry a nonce")
	}

	return aead.Open(nil, req.Ciphertext[:size], req.Ciphertext[size:], ad)
}

func (s *stubSealer) requests() []ext.OpenRequest {
	s.mu.Lock()
	defer s.mu.Unlock()

	return append([]ext.OpenRequest(nil), s.seen...)
}

func (f funcSealer) Seal(ctx context.Context, req ext.SealRequest) (ext.SealedKey, error) {
	if f.seal == nil {
		return ext.SealedKey{Ciphertext: []byte("sealed")}, nil
	}

	return f.seal(ctx, req)
}

func (f funcSealer) Open(ctx context.Context, req ext.OpenRequest) ([]byte, error) {
	if f.open == nil {
		return []byte("opened"), nil
	}

	return f.open(ctx, req)
}

func TestSealWrapperRoundTrip(t *testing.T) {
	t.Parallel()

	dek := bytes.Repeat([]byte{0x01}, 32)

	for _, tt := range []struct {
		name    string
		version string
		opaque  []byte
	}{
		{"versioned", "v2", nil},
		{"unversioned", "", nil},
		{"opaque only", "", []byte("tenant-7")},
		{"both", "v2", []byte("tenant-7")},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			sealer := &stubSealer{key: wrappingKey, version: tt.version, opaque: tt.opaque}
			w, err := ext.NewSealWrapper(sealer)
			require.NoError(t, err)

			sealed, err := w.Wrap(t.Context(), "orders", dek)
			require.NoError(t, err)

			km, err := extv1.UnmarshalKeyMaterial(sealed)
			require.NoError(t, err)
			require.Equal(t, "orders", km.GetNamespace())
			require.Equal(t, tt.version, km.GetVersion())
			require.Equal(t, tt.opaque, km.GetOpaque())
			require.Equal(t, extv1.KeyMaterial_CIPHER_UNSPECIFIED, km.GetCipher())
			require.Empty(t, km.GetNonce(), "the wrapper never writes one")
			require.NotContains(t, string(km.GetEncryptedDek()), string(dek))

			got, err := w.Unwrap(t.Context(), sealed)
			require.NoError(t, err)
			require.Equal(t, dek, got)
		})
	}
}

// TestSealWrapperBindsClearFields exercises the stub's use of
// [ext.BindingContext], not anything the wrapper does. The wrapper holds no key
// and authenticates nothing; a sealer that ignored BindingContext would open
// every one of these happily, which is the whole reason the caveat sits on
// [ext.KeySealer] rather than in a comment somewhere.
func TestSealWrapperBindsClearFields(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		name   string
		tamper func(*extv1.KeyMaterial)
	}{
		{"namespace", func(km *extv1.KeyMaterial) { km.Namespace = "payments" }},
		{"version", func(km *extv1.KeyMaterial) { km.Version = "v3" }},
		{"opaque", func(km *extv1.KeyMaterial) { km.Opaque = []byte("tenant-8") }},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			sealer := &stubSealer{key: wrappingKey, version: "v2", opaque: []byte("tenant-7")}
			w, err := ext.NewSealWrapper(sealer)
			require.NoError(t, err)

			sealed, err := w.Wrap(t.Context(), "orders", bytes.Repeat([]byte{0x01}, 32))
			require.NoError(t, err)

			_, err = w.Unwrap(t.Context(), remarshal(t, sealed, tt.tamper))
			require.ErrorContains(t, err, "failed to open key material")
		})
	}
}

// TestSealWrapperUnwrapRejects also pins which layer picks the code. Only the
// checks the wrapper makes itself carry one; the rest reach [kmsService.Decrypt]
// codeless and take InvalidArgument from implError there.
func TestSealWrapperUnwrapRejects(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		name     string
		material func(t *testing.T, sealed []byte) []byte
		msg      string
		code     codes.Code
	}{
		{
			name: "material that is not a KeyMaterial at all",
			material: func(*testing.T, []byte) []byte {
				return []byte{0xff, 0xff, 0xff}
			},
			msg:  "failed to unmarshal key material",
			code: codes.Unknown,
		},
		{
			name: "material carrying no wrapped DEK",
			material: func(*testing.T, []byte) []byte {
				return nil
			},
			msg:  "invalid key material",
			code: codes.Unknown,
		},
		{
			name: "material carrying a nonce this wrapper never writes",
			material: func(t *testing.T, sealed []byte) []byte {
				return remarshal(t, sealed, func(km *extv1.KeyMaterial) {
					km.Nonce = bytes.Repeat([]byte{0x00}, 12)
				})
			},
			msg:  "carries a nonce",
			code: codes.InvalidArgument,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			w, err := ext.NewSealWrapper(&stubSealer{key: wrappingKey})
			require.NoError(t, err)

			sealed, err := w.Wrap(t.Context(), "orders", bytes.Repeat([]byte{0x01}, 32))
			require.NoError(t, err)

			_, err = w.Unwrap(t.Context(), tt.material(t, sealed))
			require.ErrorContains(t, err, tt.msg)
			require.Equal(t, tt.code, status.Code(err))
		})
	}
}

// TestWrappersRejectEachOthersMaterial is the property an operator leans on when
// moving between the two: neither can be fed the other's material and quietly
// do something wrong with it.
func TestWrappersRejectEachOthersMaterial(t *testing.T) {
	t.Parallel()

	dek := bytes.Repeat([]byte{0x01}, 32)

	keys, err := ext.NewKeyWrapper(staticKey(wrappingKey))
	require.NoError(t, err)

	seals, err := ext.NewSealWrapper(&stubSealer{key: wrappingKey})
	require.NoError(t, err)

	t.Run("a seal wrapper refuses key wrapper material", func(t *testing.T) {
		t.Parallel()

		sealed, err := keys.Wrap(t.Context(), "orders", dek)
		require.NoError(t, err)

		_, err = seals.Unwrap(t.Context(), sealed)
		require.ErrorContains(t, err, "names cipher CIPHER_AES_256_GCM")
		require.Equal(t, codes.InvalidArgument, status.Code(err))
	})

	t.Run("a key wrapper refuses seal wrapper material", func(t *testing.T) {
		t.Parallel()

		sealed, err := seals.Wrap(t.Context(), "orders", dek)
		require.NoError(t, err)

		_, err = keys.Unwrap(t.Context(), sealed)
		require.ErrorContains(t, err, "names no cipher")
		require.Equal(t, codes.InvalidArgument, status.Code(err))
	})
}

// TestSealWrapperReportsSealerFailure keeps the sealer's own code alive across
// the wrap. Only the sealer knows whether a retry could help, so flattening its
// status here would cost the proxy the difference between a dead end and an
// outage.
func TestSealWrapperReportsSealerFailure(t *testing.T) {
	t.Parallel()

	dek := bytes.Repeat([]byte{0x01}, 32)
	down := status.Error(codes.Unavailable, "key service is down")

	working, err := ext.NewSealWrapper(&stubSealer{key: wrappingKey})
	require.NoError(t, err)

	sealed, err := working.Wrap(t.Context(), "orders", dek)
	require.NoError(t, err)

	for _, tt := range []struct {
		name   string
		sealer ext.KeySealer
		run    func(t *testing.T, w ext.KMS) error
		msg    string
		code   codes.Code
	}{
		{
			name: "sealing",
			sealer: funcSealer{seal: func(context.Context, ext.SealRequest) (ext.SealedKey, error) {
				return ext.SealedKey{}, down
			}},
			run: func(t *testing.T, w ext.KMS) error {
				_, err := w.Wrap(t.Context(), "orders", dek)

				return err
			},
			msg:  "failed to seal the DEK",
			code: codes.Unavailable,
		},
		{
			name: "opening",
			sealer: funcSealer{open: func(context.Context, ext.OpenRequest) ([]byte, error) {
				return nil, down
			}},
			run: func(t *testing.T, w ext.KMS) error {
				_, err := w.Unwrap(t.Context(), sealed)

				return err
			},
			msg:  "failed to open key material",
			code: codes.Unavailable,
		},
		{
			name: "a bare error leaves the code to the handler",
			sealer: funcSealer{seal: func(context.Context, ext.SealRequest) (ext.SealedKey, error) {
				return ext.SealedKey{}, errors.New("no idea")
			}},
			run: func(t *testing.T, w ext.KMS) error {
				_, err := w.Wrap(t.Context(), "orders", dek)

				return err
			},
			msg:  "failed to seal the DEK",
			code: codes.Unknown,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			w, err := ext.NewSealWrapper(tt.sealer)
			require.NoError(t, err)

			err = tt.run(t, w)
			require.ErrorContains(t, err, tt.msg)
			require.Equal(t, tt.code, status.Code(err))
		})
	}
}

// TestSealWrapperRejectsEmptyResults covers the two shapes a [keyWrapper]
// structurally cannot produce, since an AEAD neither seals nor opens into
// nothing. A sealer that drops an error can, and an empty DEK cached by the
// proxy would surface as garbled payloads somewhere unrelated.
func TestSealWrapperRejectsEmptyResults(t *testing.T) {
	t.Parallel()

	dek := bytes.Repeat([]byte{0x01}, 32)

	working, err := ext.NewSealWrapper(&stubSealer{key: wrappingKey})
	require.NoError(t, err)

	sealed, err := working.Wrap(t.Context(), "orders", dek)
	require.NoError(t, err)

	t.Run("sealed into no ciphertext", func(t *testing.T) {
		t.Parallel()

		w, err := ext.NewSealWrapper(funcSealer{
			seal: func(context.Context, ext.SealRequest) (ext.SealedKey, error) {
				return ext.SealedKey{Version: "v2"}, nil
			},
		})
		require.NoError(t, err)

		_, err = w.Wrap(t.Context(), "orders", dek)
		require.ErrorContains(t, err, "sealed the DEK into no ciphertext")
		require.Equal(t, codes.Internal, status.Code(err))
	})

	t.Run("opened into no DEK", func(t *testing.T) {
		t.Parallel()

		w, err := ext.NewSealWrapper(funcSealer{
			open: func(context.Context, ext.OpenRequest) ([]byte, error) {
				return nil, nil
			},
		})
		require.NoError(t, err)

		_, err = w.Unwrap(t.Context(), sealed)
		require.ErrorContains(t, err, "opened the material into no DEK")
		require.Equal(t, codes.Internal, status.Code(err))
	})
}

// TestSealWrapperRejectsAnUnframeableVersion stops the wrapper writing material
// that seals and can never open: the sealer's own [ext.BindingContext] call
// would fail on the way back, and by then the DEK is gone. Mirrors
// TestKeyWrapperRejectsAnUnframeableVersion.
func TestSealWrapperRejectsAnUnframeableVersion(t *testing.T) {
	t.Parallel()

	w, err := ext.NewSealWrapper(funcSealer{
		seal: func(context.Context, ext.SealRequest) (ext.SealedKey, error) {
			return ext.SealedKey{
				Ciphertext: []byte("sealed"),
				Version:    strings.Repeat("v", math.MaxUint16+1),
			}, nil
		},
	})
	require.NoError(t, err)

	_, err = w.Wrap(t.Context(), "orders", bytes.Repeat([]byte{0x01}, 32))
	require.ErrorContains(t, err, "key version is too long")
}

// TestSealWrapperPassesBackWhatItWasGiven is the contract the type exists for:
// whatever the sealer reported on the way in reaches it again on the way out,
// since Decrypt carries nothing else the sealer could find its key with.
func TestSealWrapperPassesBackWhatItWasGiven(t *testing.T) {
	t.Parallel()

	sealer := &stubSealer{key: wrappingKey, version: "v2", opaque: []byte("tenant-7")}
	w, err := ext.NewSealWrapper(sealer)
	require.NoError(t, err)

	sealed, err := w.Wrap(t.Context(), "orders", bytes.Repeat([]byte{0x01}, 32))
	require.NoError(t, err)
	require.Empty(t, sealer.requests(), "wrapping asks nothing of Open")

	_, err = w.Unwrap(t.Context(), sealed)
	require.NoError(t, err)

	got := sealer.requests()
	require.Len(t, got, 1)
	require.Equal(t, "orders", got[0].Namespace)
	require.Equal(t, "v2", got[0].Version)
	require.Equal(t, []byte("tenant-7"), got[0].Opaque)
	require.NotEmpty(t, got[0].Ciphertext)
}

func TestNewSealWrapperRejects(t *testing.T) {
	t.Parallel()

	_, err := ext.NewSealWrapper(nil)
	require.ErrorContains(t, err, "a key sealer is required")
}
