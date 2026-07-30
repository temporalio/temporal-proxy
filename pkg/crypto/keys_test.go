package crypto_test

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/temporalio/temporal-proxy/pkg/crypto"
)

func TestNewKeyFactory_DefaultSchemes(t *testing.T) {
	t.Parallel()

	// A URI per default scheme. Driving the cases off DefaultSchemes means a
	// scheme added there without a fixture here fails loudly instead of quietly
	// going untested.
	uris := map[string]string{
		"awskms":        "awskms://alias/some-key",
		"azurekeyvault": "azurekeyvault://vault.vault.azure.net/keys/k/v",
		"gcpkms":        "gcpkms://projects/p/locations/l/keyRings/r/cryptoKeys/k",
		"testing":       "testing://",
	}
	for _, scheme := range crypto.DefaultSchemes() {
		uri, ok := uris[scheme]
		require.True(t, ok, "default scheme %q needs a URI fixture in this test", scheme)

		t.Run(scheme, func(t *testing.T) {
			t.Parallel()

			// Each scheme is only checked as far as reaching an opener, never as far
			// as the key existing: awskms and azurekeyvault build their clients lazily
			// and so open with no credentials at all, while gcpkms resolves
			// credentials eagerly and fails without them. "Did not fail scheme lookup"
			// is the one outcome that holds both on a laptop with cloud credentials
			// and in CI without them.
			k, err := crypto.NewKeyFactory().Create(t.Context(), uri)
			if err != nil {
				require.NotContains(t, err.Error(), "key factory not found")
				return
			}

			require.NotNil(t, k)
			t.Cleanup(func() { _ = k.Close() })
		})
	}

	// Checked last so that a default lacking a fixture reports itself by name
	// above; reaching here means every default was covered, leaving only the
	// opposite drift for this to catch.
	require.Len(t, uris, len(crypto.DefaultSchemes()), "a fixture names a scheme that is no longer a default")
}

func TestNewCloudKey_RoundTrip(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	k, err := crypto.NewCloudKey(ctx, "testing://")
	require.NoError(t, err)
	t.Cleanup(func() { _ = k.Close() })

	ct, err := k.Encrypt(ctx, "ns1", []byte("payload"))
	require.NoError(t, err)
	require.NotEqual(t, []byte("payload"), ct)

	pt, err := k.Decrypt(ctx, ct)
	require.NoError(t, err)
	require.Equal(t, []byte("payload"), pt)
}

func TestNewCloudKey_ID(t *testing.T) {
	t.Parallel()

	key := base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{1}, 32))

	tests := []struct {
		name   string
		uri    string
		wantID string
	}{
		{
			name:   "the testing scheme is rewritten to a local keeper",
			uri:    "testing://" + key,
			wantID: "base64key://" + key,
		},
		{
			name:   "any other scheme is used verbatim",
			uri:    "base64key://" + key,
			wantID: "base64key://" + key,
		},
		{
			// The ID is recorded in every DEK the key wraps, so the same key spelled
			// two ways has to resolve to one identity or DEKs sealed under one
			// spelling would not open under the other.
			name:   "the testing scheme is matched without regard to case",
			uri:    "TESTING://" + key,
			wantID: "base64key://" + key,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			k, err := crypto.NewCloudKey(t.Context(), tt.uri)
			require.NoError(t, err)
			t.Cleanup(func() { _ = k.Close() })

			require.Equal(t, tt.wantID, k.ID())
		})
	}
}

func TestNewCloudKey_Errors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		uri        string
		wantErr    string
		wantAbsent string
	}{
		{
			name: "an unregistered scheme names itself",
			uri:  "bogus://whatever",
			// The URI is the only clue to which key was misconfigured, so it has to
			// survive into the error.
			wantErr: "bogus://whatever",
		},
		{
			// Nothing else redacts on the way out, so a testing:// key that fails to
			// open is the one case where key material could reach a log.
			name:       "testing key material is redacted",
			uri:        "testing://c2hvcnQ=",
			wantErr:    "testing://<redacted>",
			wantAbsent: "c2hvcnQ=",
		},
		{
			name:       "unparseable testing key material is redacted",
			uri:        "testing://not-valid-base64!!",
			wantErr:    "testing://<redacted>",
			wantAbsent: "not-valid-base64",
		},
		{
			// Redaction keys off the scheme, so a case-sensitive match here would let
			// material through on the one path meant to withhold it.
			name:       "an upper-case testing scheme redacts too",
			uri:        "TESTING://c2hvcnQ=",
			wantErr:    "testing://<redacted>",
			wantAbsent: "c2hvcnQ=",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			k, err := crypto.NewCloudKey(t.Context(), tt.uri)
			require.Nil(t, k)
			require.ErrorContains(t, err, tt.wantErr)

			if tt.wantAbsent != "" {
				require.NotContains(t, err.Error(), tt.wantAbsent)
			}
		})
	}
}

func TestNewCloudKey_EncryptIgnoresNamespace(t *testing.T) {
	t.Parallel()

	// A CloudKey addresses one fixed key, so the namespace must not influence
	// wrapping: whatever namespace sealed a DEK, the same key opens it.
	ctx := t.Context()
	k, err := crypto.NewCloudKey(ctx, testingURI(2))
	require.NoError(t, err)
	t.Cleanup(func() { _ = k.Close() })

	for _, ns := range []string{"ns1", "ns2", ""} {
		ct, err := k.Encrypt(ctx, ns, []byte("dek"))
		require.NoError(t, err)

		pt, err := k.Decrypt(ctx, ct)
		require.NoError(t, err)
		require.Equal(t, []byte("dek"), pt)
	}
}

func TestNewCloudKey_CloseClosesTheKeeper(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	k, err := crypto.NewCloudKey(ctx, testingURI(3))
	require.NoError(t, err)
	require.NoError(t, k.Close())

	// Close is the embedded keeper's, so the proof it ran is the keeper refusing
	// further work.
	_, err = k.Encrypt(ctx, "ns1", []byte("dek"))
	require.ErrorContains(t, err, "has been closed")
}

func TestWithKeyFactoryFuncForScheme(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		register string   // the scheme as handed to the option
		uris     []string // URIs that must all reach the registered opener
	}{
		{
			name:     "a scheme of the caller's own",
			register: "vault",
			uris:     []string{"vault://secret/payments"},
		},
		{
			name:     "an upper-case URI finds a lower-case registration",
			register: "vault",
			uris:     []string{"VAULT://secret/payments"},
		},
		{
			// Registering "Vault" must not build an entry no URI can reach: Create
			// looks the scheme up in the form url.Parse hands back, which is lowercase.
			name:     "a mixed-case registration is reachable by any casing",
			register: "Vault",
			uris:     []string{"vault://secret/payments", "VAULT://secret/payments", "VaUlT://x"},
		},
		{
			// Options apply after the defaults, so the caller's opener wins over
			// NewCloudKey. A built-in would have answered with a "base64key://" ID.
			name:     "a built-in scheme is replaced",
			register: "testing",
			uris:     []string{testingURI(5)},
		},
		{
			name:     "a built-in scheme is replaced whatever the casing",
			register: "TESTING",
			uris:     []string{testingURI(6)},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var seen []string
			f := crypto.NewKeyFactory(crypto.WithKeyFactoryFuncForScheme(
				tt.register,
				func(_ context.Context, uri string) (crypto.KEK, error) {
					seen = append(seen, uri)
					return &fakeKEK{id: "opened:" + uri}, nil
				},
			))

			for _, uri := range tt.uris {
				k, err := f.Create(t.Context(), uri)
				require.NoError(t, err, "uri %q should reach the opener registered as %q", uri, tt.register)

				// The prefix is proof the caller's opener answered rather than a
				// built-in, and the URI proves it arrived intact.
				require.Equal(t, "opened:"+uri, k.ID())
			}

			// Openers receive the URI as given, not the parsed form, so they are free
			// to interpret the whole thing however they like.
			require.Equal(t, tt.uris, seen)
		})
	}
}

func TestWithKeyFactoryFuncForScheme_LastRegistrationWins(t *testing.T) {
	t.Parallel()

	f := crypto.NewKeyFactory(
		crypto.WithKeyFactoryFuncForScheme("vault", func(context.Context, string) (crypto.KEK, error) {
			return &fakeKEK{id: "first"}, nil
		}),
		crypto.WithKeyFactoryFuncForScheme("vault", func(context.Context, string) (crypto.KEK, error) {
			return &fakeKEK{id: "second"}, nil
		}),
	)

	k, err := f.Create(t.Context(), "vault://secret/payments")
	require.NoError(t, err)
	require.Equal(t, "second", k.ID())
}

func TestWithKeyFactoryFuncForScheme_OpenerErrorReachesTheCaller(t *testing.T) {
	t.Parallel()

	sentinel := errors.New("key store unreachable")
	f := crypto.NewKeyFactory(crypto.WithKeyFactoryFuncForScheme(
		"vault",
		func(context.Context, string) (crypto.KEK, error) { return nil, sentinel },
	))

	_, err := f.Create(t.Context(), "vault://secret/payments")

	// Create adds no context of its own here, so callers can match on their
	// opener's own error types.
	require.ErrorIs(t, err, sentinel)
}

func TestDefaultSchemes(t *testing.T) {
	t.Parallel()

	require.Equal(t, []string{"awskms", "azurekeyvault", "gcpkms", "testing"}, crypto.DefaultSchemes())

	for _, scheme := range crypto.DefaultSchemes() {
		// Create looks schemes up in the form url.Parse hands back, so a default
		// that was not already lowercase could never be reached.
		require.Equal(t, strings.ToLower(scheme), scheme)
	}
}

func TestDefaultSchemes_ReturnsAFreshSlice(t *testing.T) {
	t.Parallel()

	// The slice is handed to callers to validate URIs against, so one that sorts
	// or overwrites it must not reach the schemes the next factory registers.
	got := crypto.DefaultSchemes()
	got[0] = "clobbered"

	require.NotContains(t, crypto.DefaultSchemes(), "clobbered")
	require.Contains(t, crypto.DefaultSchemes(), "awskms")
}

func TestKeyFactory_Create_RoundTrip(t *testing.T) {
	t.Parallel()

	// The whole path, factory through to a usable key: the testing scheme is the
	// only default one that works without cloud credentials.
	ctx := t.Context()
	k, err := crypto.NewKeyFactory().Create(ctx, testingURI(4))
	require.NoError(t, err)
	t.Cleanup(func() { _ = k.Close() })

	ct, err := k.Encrypt(ctx, "ns1", []byte("dek"))
	require.NoError(t, err)

	pt, err := k.Decrypt(ctx, ct)
	require.NoError(t, err)
	require.Equal(t, []byte("dek"), pt)
}

func TestKeyFactory_Create_Errors(t *testing.T) {
	t.Parallel()

	key := base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{1}, 32))

	tests := []struct {
		name       string
		uri        string
		wantErr    string
		wantAbsent string
	}{
		{
			name:    "an unparseable URI",
			uri:     "bogus://%zz",
			wantErr: "failed to parse URI: bogus://%zz",
		},
		{
			// A URI that fails to parse never reaches an opener, so this is the one
			// place material could escape without the opener's own redaction. net/url
			// quotes the URI back in its error, so the reason has to be reported
			// without it.
			name:       "testing key material is redacted from a parse failure",
			uri:        "testing://" + key + "\x7f",
			wantErr:    "failed to parse URI: testing://<redacted>",
			wantAbsent: key,
		},
		{
			name:    "a scheme with no opener",
			uri:     "vault://secret/key",
			wantErr: "key factory not found for scheme: vault",
		},
		{
			// url.Parse takes this as a bare path, not a scheme, so it lands in the
			// same place as an unknown scheme rather than being reported as unparseable.
			name:    "a URI with no scheme at all",
			uri:     "just-a-string",
			wantErr: "key factory not found for scheme: ",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			k, err := crypto.NewKeyFactory().Create(t.Context(), tt.uri)
			require.Nil(t, k)
			require.ErrorContains(t, err, tt.wantErr)

			if tt.wantAbsent != "" {
				require.NotContains(t, err.Error(), tt.wantAbsent)
			}
		})
	}
}

func TestKeyFactory_Create_IsSafeForConcurrentUse(t *testing.T) {
	t.Parallel()

	// A factory is registered once and then shared, so Create has to stay
	// read-only. Nothing here is timing-dependent, so we rely on the race detector
	// rather than synctest. It fails if Create ever starts writing to the scheme
	// map, memoizing openers for instance.
	f := crypto.NewKeyFactory()

	var wg sync.WaitGroup
	for i := range 8 {
		wg.Go(func() {
			k, err := f.Create(t.Context(), testingURI(byte(i)))
			require.NoError(t, err)
			require.NoError(t, k.Close())
		})
	}

	wg.Wait()
}

// testingURI builds a "testing://" URI carrying a fixed 32-byte key, so the
// rewrite to "base64key://" is observable in the resulting ID, suffix and all.
func testingURI(b byte) string {
	return "testing://" + base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{b}, 32))
}
