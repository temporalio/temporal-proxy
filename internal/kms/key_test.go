package kms

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
)

func TestNewKEK_RoundTrip(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	k, err := newKEK(ctx, "testing://")
	require.NoError(t, err)
	t.Cleanup(func() { _ = k.Close() })

	ct, err := k.Encrypt(ctx, "ns1", []byte("payload"))
	require.NoError(t, err)

	pt, err := k.Decrypt(ctx, ct)
	require.NoError(t, err)
	require.Equal(t, []byte("payload"), pt)
}

func TestNewKEK_RewritesTestingSchemeInID(t *testing.T) {
	t.Parallel()

	// A fixed 32-byte key so the testing:// -> base64key:// rewrite is observable
	// in the ID, suffix and all.
	key := base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{1}, 32))

	k, err := newKEK(t.Context(), "testing://"+key)
	require.NoError(t, err)
	t.Cleanup(func() { _ = k.Close() })

	require.Equal(t, "base64key://"+key, k.ID())
}

func TestNewKEK_InvalidURI(t *testing.T) {
	t.Parallel()

	_, err := newKEK(t.Context(), "bogus://whatever")
	require.ErrorContains(t, err, "bogus://whatever")
}

// stubConn stands in for a connection to an extension server. These tests only
// check how a key URI resolves to a connection, never what travels over it, so
// both methods are unreachable and say so rather than returning a zero value.
type stubConn struct{}

func (stubConn) Invoke(context.Context, string, any, any, ...grpc.CallOption) error {
	return errors.New("unexpected Invoke on a stub connection")
}

func (stubConn) NewStream(
	context.Context, *grpc.StreamDesc, string, ...grpc.CallOption,
) (grpc.ClientStream, error) {
	return nil, errors.New("unexpected NewStream on a stub connection")
}

// extensionConns builds a Connections entry per name.
func extensionConns(names ...string) api.Connections {
	conns := make(api.Connections, len(names))
	for _, name := range names {
		conns[name] = stubConn{}
	}

	return conns
}

func mustParse(t *testing.T, raw string) *url.URL {
	t.Helper()

	u, err := url.Parse(raw)
	require.NoError(t, err)

	return u
}

func TestNewKEKForURI_Extension(t *testing.T) {
	t.Parallel()

	conns := extensionConns("audit", "hsm")

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
			// url.Parse normalizes the scheme, so the ID is the lowercased form.
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

			k, err := newKEKForURI(t.Context(), mustParse(t, tt.uri), conns)
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)
				return
			}

			require.NoError(t, err)
			require.Equal(t, tt.wantID, k.ID())
		})
	}
}

func TestNewKEKForURI_ExtensionKeysShareAServer(t *testing.T) {
	t.Parallel()

	// Several keys may live on one extension server. They must be distinct KEKs
	// (the registry keys decryption off the ID, and DEKs record it) even though
	// they resolve to the same connection.
	conns := extensionConns("audit")

	payments, err := newKEKForURI(t.Context(), mustParse(t, "extension://audit/payments"), conns)
	require.NoError(t, err)

	billing, err := newKEKForURI(t.Context(), mustParse(t, "extension://audit/billing"), conns)
	require.NoError(t, err)

	require.NotEqual(t, payments.ID(), billing.ID())
}

func TestNewKEKForURI_NonExtensionFallsThroughToAKeeper(t *testing.T) {
	t.Parallel()

	key := base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{7}, 32))

	k, err := newKEKForURI(t.Context(), mustParse(t, "testing://"+key), nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = k.Close() })

	require.Equal(t, "base64key://"+key, k.ID())
}
