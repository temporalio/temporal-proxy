package kms

import (
	"bytes"
	"encoding/base64"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNewKEK_RoundTrip(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	k, err := newKEK(ctx, "testing://")
	require.NoError(t, err)
	t.Cleanup(func() { _ = k.Close() })

	ct, err := k.Encrypt(ctx, []byte("payload"))
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
