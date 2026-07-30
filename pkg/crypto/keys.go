package crypto

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strings"

	"gocloud.dev/secrets"
	_ "gocloud.dev/secrets/awskms"
	_ "gocloud.dev/secrets/azurekeyvault"
	_ "gocloud.dev/secrets/gcpkms"
	_ "gocloud.dev/secrets/localsecrets"
)

// testingScheme addresses a local in-process key, rewritten to gocloud's
// "base64key://" keeper by [NewCloudKey].
const testingScheme = "testing://"

type (
	// KeyFactory opens [KEK]s from key URIs, choosing an opener by URI scheme.
	// It handles the cloud KMS schemes out of the box (see [NewKeyFactory]) and
	// takes additional or replacement schemes through
	// [WithKeyFactoryFuncForScheme], so a caller can serve keys from its own key
	// store without the code consuming those KEKs knowing where they come from.
	//
	// Schemes are registered during construction only, so a KeyFactory never
	// changes afterwards and one may be shared by any number of goroutines opening
	// keys at once.
	KeyFactory struct {
		funcs map[string]KeyFactoryFunc
	}

	// KeyFactoryFunc opens the key addressed by a URI. It is called only for URIs
	// whose scheme it was registered under, and receives the URI verbatim as it
	// was passed to [KeyFactory.Create], scheme included.
	KeyFactoryFunc func(context.Context, string) (KEK, error)

	// KeyFactoryOption configures a KeyFactory during construction.
	KeyFactoryOption interface {
		apply(*KeyFactory)
	}

	keyFactoryOpt func(*KeyFactory)

	// CloudKey is a [KEK] backed by a cloud KMS key. The embedded
	// [secrets.Keeper] supplies Decrypt and Close; CloudKey adds the ID and the
	// namespace-aware Encrypt that KEK requires.
	//
	// A Keeper addresses a single fixed key, so one CloudKey wraps DEKs for every
	// namespace it is handed. Open one CloudKey per KMS key and let a
	// [KEKRegistry] decide which namespaces map onto which key.
	CloudKey struct {
		*secrets.Keeper
		id string
	}
)

// NewKeyFactory returns a KeyFactory that opens cloud KMS keys, then applies
// opts in order. The schemes registered up front are [DefaultSchemes], all
// served by [NewCloudKey]:
//
//	awskms://         AWS KMS
//	azurekeyvault://  Azure Key Vault
//	gcpkms://         Google Cloud KMS
//	testing://        a local in-process key, for tests and local runs only
//
// The driver behind each scheme is linked in by importing this package, so no
// further imports are needed to use them.
//
// opts are applied after those defaults, which means
// [WithKeyFactoryFuncForScheme] can replace any of them as well as add schemes
// of its own.
func NewKeyFactory(opts ...KeyFactoryOption) *KeyFactory {
	schemes := DefaultSchemes()
	funcs := make(map[string]KeyFactoryFunc, len(schemes))
	for _, s := range schemes {
		funcs[s] = NewCloudKey
	}

	f := &KeyFactory{funcs: funcs}
	for _, opt := range opts {
		opt.apply(f)
	}

	return f
}

// NewCloudKey opens the cloud KMS key addressed by uri, which must use one of
// the schemes listed in [NewKeyFactory]. Close the returned key when it is no
// longer needed; a [KEKRegistry] does that for the keys it holds.
//
// The "testing://" scheme is rewritten to gocloud's "base64key://" local keeper
// so tests and local runs need no cloud KMS at all. Everything after that scheme
// is the base64-encoded 32-byte key; pass a bare "testing://" to get a random
// one. Key material is kept out of the errors this function returns, but it
// still reaches the key's ID, and therefore every DEK the key wraps, which is
// one more reason to keep the scheme away from production.
//
// For every other scheme the ID is just uri, so it is stable across processes
// and identifies the key again on the decrypt path. Schemes are matched without
// regard to case, so the ID of a testing key is the same however its scheme was
// spelled.
func NewCloudKey(ctx context.Context, uri string) (KEK, error) {
	open, material := uri, ""
	if after, ok := cutPrefixFold(uri, testingScheme); ok {
		open, material = "base64key://"+after, after
	}

	kp, err := secrets.OpenKeeper(ctx, open)
	if err != nil {
		// A failure to open reports the URI it was given, so for the testing scheme
		// the key material has to come out of the cause as well as the message.
		// That costs the wrap: %w would put the material straight back. Only the
		// testing scheme pays it, leaving real KMS errors unwrappable by callers.
		if material != "" {
			return nil, fmt.Errorf(
				"failed to create key for uri: %s, %s",
				safeKeyURI(uri),
				strings.ReplaceAll(err.Error(), material, "<redacted>"),
			)
		}

		return nil, fmt.Errorf("failed to create key for uri: %s, %w", safeKeyURI(uri), err)
	}

	return &CloudKey{
		Keeper: kp,
		id:     open,
	}, nil
}

// WithKeyFactoryFuncForScheme registers fn as the opener for scheme, replacing
// whatever was registered for it before, including the built-in cloud KMS
// schemes. URI schemes are case-insensitive, so scheme is lowercased on the way
// in and matches however a caller spells it in a URI.
func WithKeyFactoryFuncForScheme(scheme string, fn KeyFactoryFunc) KeyFactoryOption {
	return keyFactoryOpt(func(kf *KeyFactory) {
		kf.funcs[strings.ToLower(scheme)] = fn
	})
}

// DefaultSchemes lists the URI schemes a [KeyFactory] serves with [NewCloudKey]
// before any option is applied, lowercased as [KeyFactory.Create] matches them.
// It is useful for validating a key URI ahead of opening it, so a typo in a
// scheme can be reported alongside the rest of a config rather than at the point
// the key is first needed.
//
// Each call returns a fresh slice; the caller may sort or filter it freely
// without disturbing the factory.
func DefaultSchemes() []string {
	return []string{
		"awskms",
		"azurekeyvault",
		"gcpkms",
		"testing",
	}
}

// Create opens the key addressed by uri with the [KeyFactoryFunc] registered for
// its scheme. It fails if uri does not parse or if no opener is registered for
// the scheme. Beyond that the opener decides: an unreachable or misconfigured
// key surfaces as whatever error that opener returns.
func (f *KeyFactory) Create(ctx context.Context, uri string) (KEK, error) {
	u, err := url.Parse(uri)
	if err != nil {
		// net/url quotes the whole URI back in its error, which would undo the
		// redaction below, so only its reason is kept. Escaping rules that out being
		// scrubbed: a control byte reaches the reason as a literal "\x7f".
		if perr, ok := errors.AsType[*url.Error](err); ok {
			err = perr.Err
		}

		return nil, fmt.Errorf("failed to parse URI: %s, %w", safeKeyURI(uri), err)
	}

	fn, ok := f.funcs[u.Scheme]
	if !ok {
		return nil, fmt.Errorf("key factory not found for scheme: %s", u.Scheme)
	}

	return fn(ctx, uri)
}

// ID returns a unique ID for this KEK, e.g. a KMS ARN.
func (k *CloudKey) ID() string {
	return k.id
}

// Encrypt wraps the DEK using the underlying keeper. The namespace is unused: a
// gocloud keeper addresses a single fixed key, so namespace-based selection has
// already happened by the time this KEK is chosen.
func (k *CloudKey) Encrypt(ctx context.Context, _ string, dek []byte) ([]byte, error) {
	return k.Keeper.Encrypt(ctx, dek)
}

func (f keyFactoryOpt) apply(kf *KeyFactory) { f(kf) }

// safeKeyURI returns uri with the key material a "testing://" URI carries inline
// replaced, so the URI itself is safe to report. A bare "testing://" carries none
// and is returned unchanged, as is every other scheme: those name a key rather
// than carrying it, and the name is what makes the report useful.
func safeKeyURI(uri string) string {
	if after, ok := cutPrefixFold(uri, testingScheme); ok && after != "" {
		return testingScheme + "<redacted>"
	}

	return uri
}

// cutPrefixFold is [strings.CutPrefix] with a case-insensitive match. Scheme
// prefixes need it because URI schemes are case-insensitive and callers may
// spell them any way they like, while [net/url.Parse] and gocloud both hand back
// the lowercased form.
func cutPrefixFold(s, prefix string) (string, bool) {
	if len(s) < len(prefix) || !strings.EqualFold(s[:len(prefix)], prefix) {
		return "", false
	}

	return s[len(prefix):], true
}
