package kms

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

type (
	stubKEK struct {
		err error
	}

	recordedKEKOp struct {
		provider  string
		operation string
		result    string
	}

	fakeRecorder struct {
		ops []recordedKEKOp
	}
)

func TestMeteredKEKRecordsWrap(t *testing.T) {
	t.Parallel()

	r := &fakeRecorder{}
	k := newMeteredKEK(stubKEK{}, "aws", r)

	out, err := k.Encrypt(t.Context(), "ns1", []byte("dek"))
	require.NoError(t, err)
	require.Equal(t, []byte("dek"), out) // result passed through unchanged

	require.Equal(t, []recordedKEKOp{{"aws", "wrap", "success"}}, r.ops)
}

func TestMeteredKEKRecordsUnwrapError(t *testing.T) {
	t.Parallel()

	r := &fakeRecorder{}
	sentinel := errors.New("boom")
	k := newMeteredKEK(stubKEK{err: sentinel}, "gcp", r)

	_, err := k.Decrypt(t.Context(), []byte("x"))
	require.ErrorIs(t, err, sentinel) // error passed through unchanged

	require.Equal(t, []recordedKEKOp{{"gcp", "unwrap", "error"}}, r.ops)
}

func TestProviderForScheme(t *testing.T) {
	t.Parallel()

	cases := map[string]string{
		"awskms":        "aws",
		"gcpkms":        "gcp",
		"azurekeyvault": "azure",
		"testing":       "testing",
		"base64key":     "testing",
		"weird":         "weird",
		// Every extension server shares one label; per-server labels would make
		// the metric's cardinality track the configuration.
		ExtensionScheme: ExtensionScheme,
	}
	for scheme, want := range cases {
		require.Equal(t, want, providerForScheme(scheme), "scheme %q", scheme)
	}
}

func (s stubKEK) ID() string { return "stub" }
func (s stubKEK) Encrypt(_ context.Context, _ string, b []byte) ([]byte, error) {
	if s.err != nil {
		return nil, s.err
	}
	return b, nil
}

func (s stubKEK) Decrypt(_ context.Context, b []byte) ([]byte, error) {
	if s.err != nil {
		return nil, s.err
	}
	return b, nil
}
func (s stubKEK) Close() error { return nil }

func (f *fakeRecorder) KEKOp(provider, operation, result string, _ float64) {
	f.ops = append(f.ops, recordedKEKOp{provider, operation, result})
}
