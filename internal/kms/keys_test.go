package kms_test

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"net/url"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/temporalio/temporal-proxy/internal/api"
	"github.com/temporalio/temporal-proxy/internal/kms"
	"github.com/temporalio/temporal-proxy/pkg/crypto"
	"github.com/temporalio/temporal-proxy/pkg/logger"
	"github.com/temporalio/temporal-proxy/pkg/logger/tag"
)

type (
	// stubConn stands in for a connection to an extension server. Nothing here
	// checks what travels over one, so both methods are unreachable and say so
	// rather than returning a zero value.
	stubConn struct{}

	recordedKEKOp struct {
		provider  string
		operation string
		result    string
	}

	// fakeRecorder satisfies the recorder kms.NewKeyFactory takes. That interface is
	// unexported, but its method is not, so a value from outside the package still
	// satisfies it.
	fakeRecorder struct {
		ops []recordedKEKOp
	}
)

func TestNewKeyFactory_ServesDefaultAndExtensionSchemes(t *testing.T) {
	t.Parallel()

	f := kms.NewKeyFactory(extensionConns("audit"), &fakeRecorder{})

	// The extension scheme is the one this factory adds; the testing scheme proves
	// registering it did not displace what crypto already handled.
	for _, uri := range []string{"extension://audit/payments", "testing://"} {
		k, err := f.Create(t.Context(), uri)
		require.NoError(t, err, "uri %q", uri)
		t.Cleanup(func() { _ = k.Close() })
	}

	_, err := f.Create(t.Context(), "bogus://whatever")
	require.ErrorContains(t, err, "key factory not found for scheme: bogus")
}

func TestKeyFactory_Create_MetersWrapAndUnwrap(t *testing.T) {
	t.Parallel()

	r := &fakeRecorder{}
	k := mustCreate(t, kms.NewKeyFactory(nil, r), "testing://")

	ct, err := k.Encrypt(t.Context(), "ns1", []byte("dek"))
	require.NoError(t, err)

	pt, err := k.Decrypt(t.Context(), ct)
	require.NoError(t, err)
	require.Equal(t, []byte("dek"), pt) // metering returns the call's result verbatim

	// Decrypting something that is not ciphertext exercises the failure label.
	_, err = k.Decrypt(t.Context(), []byte("not-ciphertext"))
	require.Error(t, err)

	require.Equal(t, []recordedKEKOp{
		{"testing", "wrap", "success"},
		{"testing", "unwrap", "success"},
		{"testing", "unwrap", "error"},
	}, r.ops)
}

func TestKeyFactory_Create_ProviderLabel(t *testing.T) {
	t.Parallel()

	// Every key is wrapped once and the label it reported is read back off the
	// recorder. Whether the wrap succeeds does not matter, which is what makes the
	// cloud schemes reachable: the context is cancelled first, so they fail before
	// any network call. gcpkms cannot appear here because it resolves credentials
	// when opened, so its label is covered by TestProviderForScheme instead.
	tests := []struct {
		name         string
		uri          string
		wantProvider string
	}{
		{name: "testing", uri: "testing://", wantProvider: "testing"},
		{name: "extension", uri: "extension://audit/payments", wantProvider: "extension"},
		{name: "awskms", uri: "awskms://alias/some-key", wantProvider: "aws"},
		{
			name:         "azurekeyvault",
			uri:          "azurekeyvault://vault.vault.azure.net/keys/k/v",
			wantProvider: "azure",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			r := &fakeRecorder{}
			k := mustCreate(t, kms.NewKeyFactory(extensionConns("audit"), r), tt.uri)

			ctx, cancel := context.WithCancel(t.Context())
			cancel()

			_, _ = k.Encrypt(ctx, "ns1", []byte("dek"))

			require.Len(t, r.ops, 1)
			require.Equal(t, tt.wantProvider, r.ops[0].provider)
		})
	}
}

func TestKeyFactory_Create_PassesThroughIDAndClose(t *testing.T) {
	t.Parallel()

	// ID and Close belong to the key crypto opened, so metering must not shadow
	// them: the registry keys decryption off the ID, and Close releases the key.
	k := mustCreate(t, kms.NewKeyFactory(nil, &fakeRecorder{}), "testing://")

	require.Equal(t, "base64key://", k.ID())
	require.NoError(t, k.Close())
}

func TestKeyFactory_Create_ErrorNamesTheKey(t *testing.T) {
	t.Parallel()

	f := kms.NewKeyFactory(nil, &fakeRecorder{})

	// An unopenable key has to say which URI failed, and a testing key's material
	// must not travel with it.
	_, err := f.Create(t.Context(), "testing://c2hvcnQ=")
	require.ErrorContains(t, err, "testing://<redacted>")
	require.NotContains(t, err.Error(), "c2hvcnQ=")
}

func TestKeyFactory_Create_ExtensionURIs(t *testing.T) {
	t.Parallel()

	f := kms.NewKeyFactory(extensionConns("audit", "hsm"), &fakeRecorder{})

	tests := []struct {
		name    string
		uri     string
		wantID  string
		wantErr string
	}{
		{
			name:   "resolves a configured server, identified by the whole URI",
			uri:    "extension://audit/payments",
			wantID: "extension://audit/payments",
		},
		{
			name:   "a key id is optional",
			uri:    "extension://audit",
			wantID: "extension://audit",
		},
		{
			name:   "a dotted key id needs no escaping",
			uri:    "extension://audit/billing.v2",
			wantID: "extension://audit/billing.v2",
		},
		{
			// url.Parse normalizes the scheme, so the ID is the lowercased form. It is
			// recorded in every DEK the key wraps, so it must not vary with spelling.
			name:   "the scheme is matched case-insensitively",
			uri:    "EXTENSION://hsm/root",
			wantID: "extension://hsm/root",
		},
		{
			name:    "an unconfigured server is rejected by name",
			uri:     "extension://missing/payments",
			wantErr: `unknown extension server "missing"`,
		},
		{
			name:    "a URI with no server is rejected",
			uri:     "extension:///payments",
			wantErr: "must name an extension server",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			k, err := f.Create(t.Context(), tt.uri)
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)
				return
			}

			require.NoError(t, err)
			t.Cleanup(func() { _ = k.Close() })

			require.Equal(t, tt.wantID, k.ID())
		})
	}
}

func TestKeyFactory_Create_ExtensionKeysShareAServer(t *testing.T) {
	t.Parallel()

	// Several keys may live on one extension server. They must be distinct KEKs
	// (the registry keys decryption off the ID, and DEKs record it) even though
	// they resolve to the same connection.
	f := kms.NewKeyFactory(extensionConns("audit"), &fakeRecorder{})

	payments := mustCreate(t, f, "extension://audit/payments")
	billing := mustCreate(t, f, "extension://audit/billing")

	require.NotEqual(t, payments.ID(), billing.ID())
}

func TestKeyFactory_Create_ExtensionWithoutConnections(t *testing.T) {
	t.Parallel()

	// Connections are resolved when a key is opened, not when the factory is
	// built, so an empty pool fails here rather than at construction.
	f := kms.NewKeyFactory(nil, &fakeRecorder{})

	_, err := f.Create(t.Context(), "extension://audit/payments")
	require.ErrorContains(t, err, `unknown extension server "audit"`)
}

func TestProviderForScheme(t *testing.T) {
	t.Parallel()

	cases := map[string]string{
		"awskms":        "aws",
		"gcpkms":        "gcp",
		"azurekeyvault": "azure",
		"testing":       "testing",
		// crypto rewrites testing:// keys to base64key://, so both must report the
		// same provider.
		"base64key": "testing",
		"weird":     "weird",
		// Schemes reach this from a URI, which may be spelled any way.
		"AWSKMS": "aws",
		// Every extension server shares one label; per-server labels would make
		// the metric's cardinality track the configuration.
		kms.ExtensionScheme: kms.ExtensionScheme,
	}

	for scheme, want := range cases {
		require.Equal(t, want, kms.ProviderForScheme(scheme), "scheme %q", scheme)
	}
}

func TestModule_RedactsTestingKeyMaterialWhenLoggingKeys(t *testing.T) {
	t.Parallel()

	// A testing:// URI carries its key inline, so the line announcing each key must
	// not reproduce it. Asserting on the log is the only way to see this: the URI
	// is logged, never returned.
	log := logger.NewTestLogger()

	_, err := buildVault(t, encryptionConfig(true, keyPolicy(t, 1)), log, nil)
	require.NoError(t, err)

	require.True(
		t,
		log.ContainsEntry(
			logger.LevelInfo,
			"Registering crypto key",
			tag.String("namespace", "default"),
			tag.String("uri", "testing://<redacted>"),
		),
		"the key URI should be logged with its material redacted",
	)
}

func (stubConn) Invoke(context.Context, string, any, any, ...grpc.CallOption) error {
	return errors.New("unexpected Invoke on a stub connection")
}

func (stubConn) NewStream(
	context.Context, *grpc.StreamDesc, string, ...grpc.CallOption,
) (grpc.ClientStream, error) {
	return nil, errors.New("unexpected NewStream on a stub connection")
}

func (f *fakeRecorder) KEKOp(provider, operation, result string, _ float64) {
	f.ops = append(f.ops, recordedKEKOp{provider, operation, result})
}

// extensionConns builds a Connections entry per name, each a connection that
// fails any call made over it. These tests only check how a key URI resolves to a
// connection, never what travels over it.
func extensionConns(names ...string) api.Connections {
	conns := make(api.Connections, len(names))
	for _, name := range names {
		conns[name] = stubConn{}
	}

	return conns
}

func mustCreate(t *testing.T, f *kms.KeyFactory, uri string) crypto.KEK {
	t.Helper()

	k, err := f.Create(t.Context(), uri)
	require.NoError(t, err)
	t.Cleanup(func() { _ = k.Close() })

	return k
}

// testingKeyURL builds a testing:// key URI whose 32-byte key is filled with b.
func testingKeyURL(t *testing.T, b byte) url.URL {
	t.Helper()

	key := base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{b}, 32))
	u, err := url.Parse("testing://" + key)
	require.NoError(t, err)

	return *u
}
