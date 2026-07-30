package kms

import (
	"context"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/temporalio/temporal-proxy/internal/api"
	"github.com/temporalio/temporal-proxy/pkg/crypto"
)

// ExtensionScheme addresses a key served by a configured extension server, as
// "extension://<server>/<key>". The server names an entry in the config's
// extensionServers list; the key is a proxy-side identifier that distinguishes
// several keys hosted by one server. It is never sent to the server, which
// selects keys by namespace when wrapping and reads the key back out of the
// self-describing ciphertext when unwrapping.
const ExtensionScheme = "extension"

// providerMap renames the KMS URI schemes whose provider label differs from the
// scheme itself. A scheme that is absent is used as its own label, so a backend
// added to crypto still reports something usable.
var providerMap = map[string]string{
	"awskms":        "aws",
	"gcpkms":        "gcp",
	"azurekeyvault": "azure",
	"base64key":     "testing",
	"testing":       "testing",
}

type (
	// KeyFactory opens the proxy's configured keys, adding two things to
	// [crypto.KeyFactory]: the "extension://" scheme, which resolves to an
	// operator-run extension server rather than a cloud KMS, and metering, so
	// every wrap and unwrap a key performs is recorded against its provider.
	KeyFactory struct {
		kf *crypto.KeyFactory
		r  kekRecorder
	}

	// meteredKEK decorates a crypto.KEK, recording each wrap (Encrypt) and unwrap
	// (Decrypt) as a KEK operation. It embeds the wrapped KEK so ID and Close pass
	// through unchanged, and returns the wrapped call's result and error verbatim so
	// metering never alters behavior.
	meteredKEK struct {
		crypto.KEK
		provider string
		recorder kekRecorder
	}

	// kekRecorder records KEK wrap/unwrap operations. *Reporter satisfies it; the
	// interface lets meteredKEK be tested without a Prometheus-backed reporter.
	kekRecorder interface {
		KEKOp(provider, operation, result string, seconds float64)
	}
)

// NewKeyFactory returns a KeyFactory serving the schemes crypto handles by
// default plus [ExtensionScheme], which resolves against conns. Keys it opens
// record their operations to r.
//
// conns is captured, not copied, and is looked up when a key is opened rather
// than here, so an extension URI naming a server absent from conns fails at that
// point and not at construction.
func NewKeyFactory(conns api.Connections, r kekRecorder) *KeyFactory {
	return &KeyFactory{
		kf: crypto.NewKeyFactory(
			crypto.WithKeyFactoryFuncForScheme(ExtensionScheme, newExtensionKeyFunc(conns)),
		),
		r: r,
	}
}

// ProviderForScheme maps a KMS URI scheme to a stable, low-cardinality provider
// label. Unknown schemes are used unchanged so a new backend still produces a
// usable (if unrecognized) label rather than an empty one.
func ProviderForScheme(scheme string) string {
	scheme = strings.ToLower(scheme)
	if p, ok := providerMap[scheme]; ok {
		return p
	}

	return scheme
}

// Create opens the key addressed by uri and wraps it so its KMS calls are
// metered. The provider label is derived from the URI's scheme, which keeps the
// metric's cardinality tied to the number of backends rather than to the number
// of configured keys.
func (f *KeyFactory) Create(ctx context.Context, uri string) (crypto.KEK, error) {
	k, err := f.kf.Create(ctx, uri)
	if err != nil {
		return nil, fmt.Errorf("error creating KEK: %w", err)
	}

	// The scheme is whatever precedes the first colon. Create above has already
	// rejected a URI without one, so there is nothing to guard against here.
	scheme, _, _ := strings.Cut(uri, ":")

	return &meteredKEK{
		KEK:      k,
		provider: ProviderForScheme(scheme),
		recorder: f.r,
	}, nil
}

func (m *meteredKEK) Encrypt(ctx context.Context, ns string, b []byte) ([]byte, error) {
	start := time.Now()
	ct, err := m.KEK.Encrypt(ctx, ns, b)
	m.recorder.KEKOp(m.provider, "wrap", resultLabel(err), time.Since(start).Seconds())

	return ct, err
}

func (m *meteredKEK) Decrypt(ctx context.Context, b []byte) ([]byte, error) {
	start := time.Now()
	pt, err := m.KEK.Decrypt(ctx, b)
	m.recorder.KEKOp(m.provider, "unwrap", resultLabel(err), time.Since(start).Seconds())

	return pt, err
}

// newExtensionKeyFunc returns the opener for [ExtensionScheme] URIs, resolving
// each to a KMS client on the named server's pooled connection. Several keys may
// share one server, so the whole URI (not the server name) is the KEK identity:
// it is recorded in every DEK the key wraps and is what selects the key again on
// the decrypt path.
//
// Connections are owned by the pool, so the returned client's Close is a no-op
// and any number of KEKs may safely share one server.
func newExtensionKeyFunc(conns api.Connections) crypto.KeyFactoryFunc {
	return func(_ context.Context, uri string) (crypto.KEK, error) {
		// Unreachable as registered, since the crypto factory parses a URI before
		// dispatching on its scheme. It is handled anyway because KeyFactoryFunc is a
		// public contract and nothing stops a caller invoking this directly.
		u, err := url.Parse(uri)
		if err != nil {
			return nil, fmt.Errorf("failed to parse URI: %s, %w", uri, err)
		}

		server := u.Host
		if server == "" {
			return nil, fmt.Errorf("extension key URI must name an extension server: %s", uri)
		}

		conn, ok := conns[server]
		if !ok {
			return nil, fmt.Errorf("unknown extension server %q in key URI: %s", server, uri)
		}

		// The parsed URI, not the raw one, so a key opened as "EXTENSION://..."
		// carries the same ID as the same key spelled in lower case.
		return api.NewKMS(u.String(), conn), nil
	}
}

// resultLabel maps an error to the "result" metric label value.
func resultLabel(err error) string {
	if err != nil {
		return "error"
	}

	return "success"
}

// safeKeyString redacts the key material a "testing://" URI carries inline, so a
// URI is safe to log. The scheme is matched without regard to case, since crypto
// accepts any spelling of it. Other schemes address a key by name rather than by
// value and are returned unchanged.
func safeKeyString(uri string) string {
	if len(uri) < len(testingKeyScheme) || !strings.EqualFold(uri[:len(testingKeyScheme)], testingKeyScheme) {
		return uri
	}

	return testingKeyScheme + "<redacted>"
}
