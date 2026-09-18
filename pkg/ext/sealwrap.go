package ext

import (
	"context"
	"errors"
	"fmt"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/temporalio/temporal-proxy/pkg/api/ext/v1"
)

type (
	// SealRequest names the DEK a [KeySealer] is being asked to seal. It carries
	// no opaque bytes: the sealer chooses those and reports them in [SealedKey].
	SealRequest struct {
		// Namespace is the pre-translation (local) namespace the DEK belongs to.
		Namespace string

		// DEK is the key to seal. It is key material, never a payload.
		DEK []byte
	}

	// SealedKey is what a [KeySealer] produced, and everything it gets back when
	// asked to open the same material again. Neither slice is copied, so a sealer
	// handing back a pooled buffer corrupts material it has already returned.
	SealedKey struct {
		// Ciphertext is the sealed DEK, in whatever form the key service returned.
		Ciphertext []byte

		// Version identifies the key that sealed Ciphertext, so a key service that
		// rotates can find the same one again. Empty means it does not version.
		Version string

		// Opaque belongs to the sealer. Nothing here reads it.
		Opaque []byte
	}

	// OpenRequest carries back everything the material held, so a [KeySealer] can
	// find its key and rebuild the encryption context it sealed under with
	// ext.BindingContext(req.Namespace, req.Version, req.Opaque).
	OpenRequest struct {
		Namespace  string
		Version    string
		Opaque     []byte
		Ciphertext []byte
	}

	// KeySealer seals and opens DEKs through a key service that will not hand over
	// its keys. Hand one to [NewSealWrapper].
	//
	// The wrapper authenticates nothing, and cannot: it holds no key, and the
	// version and opaque bytes it would bind are chosen by Seal, so they do not
	// exist until the call it would bind them to has already happened. An
	// implementation that ignores [BindingContext] produces material whose
	// namespace, version, and opaque can be swapped by anyone able to write a
	// payload's metadata. Passing those bytes to the key service as an encryption
	// context is what makes relabelled material fail to open instead.
	//
	// A [google.golang.org/grpc/status] error is passed through with its code
	// intact. Implementations must be safe for concurrent use.
	KeySealer interface {
		Seal(context.Context, SealRequest) (SealedKey, error)
		Open(context.Context, OpenRequest) ([]byte, error)
	}

	// sealWrapper is a [KMS] that frames what a [KeySealer] sealed as
	// [ext.KeyMaterial].
	sealWrapper struct {
		sealer KeySealer
	}
)

// NewSealWrapper returns a [KMS] that frames what a [KeySealer] seals as
// [ext.KeyMaterial], so an implementation whose key service will not release its
// keys still writes no framing of its own.
func NewSealWrapper(s KeySealer) (KMS, error) {
	if s == nil {
		return nil, errors.New("a key sealer is required")
	}

	return &sealWrapper{sealer: s}, nil
}

// Wrap has the sealer seal dek and returns it framed as [ext.KeyMaterial],
// carrying back whatever version and opaque bytes the sealer reported.
func (w *sealWrapper) Wrap(ctx context.Context, namespace string, dek []byte) ([]byte, error) {
	sealed, err := w.sealer.Seal(ctx, SealRequest{Namespace: namespace, DEK: dek})
	if err != nil {
		// Wrapped rather than replaced, so a sealer that chose a status code keeps
		// it: only the sealer knows whether a retry could help.
		return nil, fmt.Errorf("failed to seal the DEK: %w", err)
	}

	// Marshal would refuse this too, but its message names the framing rather than
	// the sealer and sends the reader somewhere unhelpful. [keyWrapper] needs no
	// equivalent check, since an AEAD cannot seal into nothing.
	if len(sealed.Ciphertext) == 0 {
		return nil, status.Error(codes.Internal, "the key service sealed the DEK into no ciphertext")
	}

	// The bytes are discarded: this wrapper binds nothing. It is a check that the
	// sealer can recompute the same context later, since a version too long to
	// encode would seal here and then never open.
	if _, err := BindingContext(namespace, sealed.Version, sealed.Opaque); err != nil {
		return nil, err
	}

	// Cipher and Nonce are left unset. The key service chose whatever construction
	// it used, and this wrapper cannot describe it.
	km := &ext.KeyMaterial{
		EncryptedDek: sealed.Ciphertext,
		Version:      sealed.Version,
		Namespace:    namespace,
		Opaque:       sealed.Opaque,
	}

	packed, err := km.Marshal()
	if err != nil {
		return nil, fmt.Errorf("failed to pack key material: %w", err)
	}

	return packed, nil
}

// Unwrap hands the material's fields back to the sealer, which finds its key and
// opens what it sealed.
func (w *sealWrapper) Unwrap(ctx context.Context, ciphertext []byte) ([]byte, error) {
	km, err := ext.UnmarshalKeyMaterial(ciphertext)
	if err != nil {
		return nil, err
	}

	// Unmarshal accepts bytes that set no fields at all, so anything arriving from
	// outside is checked before it is trusted.
	if err := km.Validate(); err != nil {
		return nil, fmt.Errorf("invalid key material: %w", err)
	}

	// A named cipher means a [keyWrapper] sealed this, not a key service. The check
	// is also what keeps the field honest: [BindingContext] encodes an unset
	// cipher, so tolerating a named one would leave that field unauthenticated and
	// unchecked both, free for anyone to flip.
	if km.GetCipher() != ext.KeyMaterial_CIPHER_UNSPECIFIED {
		return nil, status.Errorf(
			codes.InvalidArgument,
			"key material names cipher %s, so this wrapper did not seal it",
			km.GetCipher(),
		)
	}

	// Same reasoning. A nonce is never written here and never bound, and a sealer
	// that genuinely carries an IV has Opaque, which is bound.
	if len(km.GetNonce()) > 0 {
		return nil, status.Error(
			codes.InvalidArgument,
			"key material carries a nonce, which this wrapper never writes",
		)
	}

	dek, err := w.sealer.Open(ctx, OpenRequest{
		Namespace:  km.GetNamespace(),
		Version:    km.GetVersion(),
		Opaque:     km.GetOpaque(),
		Ciphertext: km.GetEncryptedDek(),
	})
	if err != nil {
		// As in Wrap: a sealer that reported Unavailable rather than a dead end is
		// the only thing that knows the difference.
		return nil, fmt.Errorf("failed to open key material: %w", err)
	}

	// An AEAD cannot open into nothing, so [keyWrapper] needs no equivalent. A
	// sealer that drops an error can, and the proxy would cache the empty DEK and
	// garble payloads somewhere else entirely.
	if len(dek) == 0 {
		return nil, status.Error(codes.Internal, "the key service opened the material into no DEK")
	}

	return dek, nil
}
