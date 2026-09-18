package ext

import (
	"context"
	"crypto/cipher"
	"crypto/rand"
	"errors"
	"fmt"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/temporalio/temporal-proxy/pkg/api/ext/v1"
)

type (
	// KeyRequest names the key a [KeyLookup] is being asked for.
	KeyRequest struct {
		// Namespace is the pre-translation (local) namespace the DEK belongs to.
		Namespace string

		// Version is the key version being asked for, and is empty when the
		// question is "whichever key is current". Sealing leaves it empty; opening
		// sets it to whatever the key material carried, which is itself empty for a
		// lookup that does not version its keys, so an unversioned store can ignore
		// this field entirely.
		Version string
	}

	// Key is a wrapping key and the version that addresses it.
	Key struct {
		// Bytes is the key itself, 32 bytes for every cipher this package ships.
		Bytes []byte

		// Version addresses Bytes again later. It is recorded in the key material
		// when sealing, and ignored when opening, where the version is already
		// known and is the one the request named.
		Version string
	}

	// KeyLookup supplies the wrapping keys [NewKeyWrapper] seals with. It is the
	// only thing NewKeyWrapper cannot supply for itself.
	//
	// A lookup must answer for every version it ever reported, not only the
	// current one: forgetting a version destroys every payload sealed under it.
	// Returning a [google.golang.org/grpc/status] error passes its code through to
	// the proxy, which is how an unreachable key store is distinguished from a
	// version that will never resolve.
	//
	// A lookup must be safe for concurrent use.
	KeyLookup func(context.Context, KeyRequest) (Key, error)

	// KeyWrapperOption configures a key wrapper during construction.
	KeyWrapperOption interface {
		apply(*keyWrapper) error
	}

	keyWrapperOpt func(*keyWrapper) error

	// keyWrapper is a [KMS] that seals DEKs with an AEAD and frames them as
	// [ext.KeyMaterial].
	keyWrapper struct {
		lookup  KeyLookup
		cipher  CipherID
		ciphers map[CipherID]CipherFunc
	}
)

// NewKeyWrapper returns a [KMS] that seals DEKs with an AEAD over keys from
// lookup and frames them as [ext.KeyMaterial], so an extension server supplies
// key material and nothing else.
//
// New material is sealed with AES-256-GCM unless [WithCipher] says otherwise,
// and carries both the cipher that sealed it and the key version lookup reported
// at the time. Opening reads those from the material rather than from the
// configuration, so changing cipher, or a key store rotating underneath, leaves
// everything already sealed readable.
//
// Ciphers are registered during construction only, so the returned KMS never
// changes afterwards and may be shared by any number of goroutines.
func NewKeyWrapper(lookup KeyLookup, opts ...KeyWrapperOption) (KMS, error) {
	if lookup == nil {
		return nil, errors.New("a key lookup is required")
	}

	w := &keyWrapper{
		lookup:  lookup,
		cipher:  CipherAES256GCM,
		ciphers: defaultCiphers(),
	}

	for _, opt := range opts {
		if err := opt.apply(w); err != nil {
			return nil, err
		}
	}

	if _, ok := w.ciphers[w.cipher]; !ok {
		return nil, fmt.Errorf("no cipher registered for: %s", w.cipher)
	}

	return w, nil
}

// WithCipher seals new material with id instead of AES-256-GCM. It has no
// bearing on opening, which uses whatever cipher the material names.
func WithCipher(id CipherID) KeyWrapperOption {
	return keyWrapperOpt(func(w *keyWrapper) error {
		w.cipher = id

		return nil
	})
}

// WithCipherFunc registers fn as the constructor for id, replacing whatever was
// registered before, including a built-in.
//
// Ids from 128 up are reserved for exactly this and will never be assigned by
// [ext.KeyMaterial_Cipher], so a cipher registered there cannot collide with
// one added later; [MustCipherID] builds one. An id below that is accepted,
// since replacing a built-in with a stricter construction of the same cipher is
// reasonable, but reusing a built-in id for a different cipher makes material
// that other servers will misread.
func WithCipherFunc(id CipherID, fn CipherFunc) KeyWrapperOption {
	return keyWrapperOpt(func(w *keyWrapper) error {
		if fn == nil {
			return fmt.Errorf("cipher func for %s must not be nil", id)
		}

		if id <= 0 {
			return fmt.Errorf("cipher id must be positive, got %d", id)
		}

		w.ciphers[id] = fn

		return nil
	})
}

// Wrap seals dek under whichever key its lookup reports as current for
// namespace, and returns it framed as [ext.KeyMaterial] carrying the version
// the lookup reported.
func (w *keyWrapper) Wrap(ctx context.Context, namespace string, dek []byte) ([]byte, error) {
	// An empty Version asks for whichever key is current, and the answer says
	// which one that turned out to be.
	key, err := w.lookup(ctx, KeyRequest{Namespace: namespace})
	if err != nil {
		// Wrapped rather than replaced, so a lookup that chose a status code keeps
		// it: only the lookup knows whether a retry could help.
		return nil, fmt.Errorf("failed to look up a sealing key: %w", err)
	}

	aead, err := w.aead(w.cipher, key.Bytes)
	if err != nil {
		return nil, err
	}

	km := &ext.KeyMaterial{
		Version:   key.Version,
		Namespace: namespace,
		Cipher:    w.cipher,
		Nonce:     make([]byte, aead.NonceSize()),
	}

	// crypto/rand.Read never returns an error; it crashes the program instead.
	_, _ = rand.Read(km.Nonce)

	ad, err := additionalData(km)
	if err != nil {
		return nil, err
	}

	km.EncryptedDek = aead.Seal(nil, km.Nonce, dek, ad)

	packed, err := km.Marshal()
	if err != nil {
		return nil, fmt.Errorf("failed to pack key material: %w", err)
	}

	return packed, nil
}

// Unwrap opens key material produced by Wrap, asking its lookup for
// the key the material's version and namespace address, and building the cipher
// the material names.
func (w *keyWrapper) Unwrap(ctx context.Context, ciphertext []byte) ([]byte, error) {
	km, err := ext.UnmarshalKeyMaterial(ciphertext)
	if err != nil {
		return nil, err
	}

	// Unmarshal accepts bytes that set no fields at all, so anything arriving from
	// outside is checked before it is trusted.
	if err := km.Validate(); err != nil {
		return nil, fmt.Errorf("invalid key material: %w", err)
	}

	// An unset cipher is refused rather than assumed to be the default. Material
	// without one was framed by a server doing its own wrapping, and guessing
	// would mean opening it under a construction nobody chose.
	if km.GetCipher() == ext.KeyMaterial_CIPHER_UNSPECIFIED {
		return nil, status.Error(codes.InvalidArgument, "key material names no cipher")
	}

	// Key.Version is ignored here: the version is already known, it is the one
	// named below, and it is what the additional data binds.
	key, err := w.lookup(ctx, KeyRequest{
		Namespace: km.GetNamespace(),
		Version:   km.GetVersion(),
	})
	if err != nil {
		// As in Wrap: a lookup that reported Unavailable rather than a dead end is
		// the only thing that knows the difference.
		return nil, fmt.Errorf("failed to look up an opening key: %w", err)
	}

	aead, err := w.aead(km.GetCipher(), key.Bytes)
	if err != nil {
		return nil, err
	}

	if len(km.GetNonce()) != aead.NonceSize() {
		return nil, status.Errorf(
			codes.InvalidArgument,
			"key material carries a %d-byte nonce, want %d",
			len(km.GetNonce()),
			aead.NonceSize(),
		)
	}

	ad, err := additionalData(km)
	if err != nil {
		return nil, err
	}

	dek, err := aead.Open(nil, km.GetNonce(), km.GetEncryptedDek(), ad)
	if err != nil {
		// The cause is deliberately not wrapped: an AEAD reports only that opening
		// failed, and repeating that says nothing the caller can act on while
		// inviting the reader to treat it as a distinguishable case.
		return nil, status.Error(codes.InvalidArgument, "failed to open key material")
	}

	return dek, nil
}

// aead builds the cipher named by id over key.
func (w *keyWrapper) aead(id CipherID, key []byte) (cipher.AEAD, error) {
	newAEAD, ok := w.ciphers[id]
	if !ok {
		return nil, status.Errorf(codes.InvalidArgument, "no cipher registered for: %s", id)
	}

	aead, err := newAEAD(key)
	if err != nil {
		return nil, err
	}

	return aead, nil
}

func (f keyWrapperOpt) apply(w *keyWrapper) error {
	return f(w)
}
