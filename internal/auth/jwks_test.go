package auth

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/rsa"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/MicahParks/jwkset"
	"github.com/golang-jwt/jwt/v5"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/temporalio/temporal-proxy/pkg/logger"
)

// localStream is a minimal grpc.ServerStream stand-in. It duplicates
// authenticator_test.go's fakeStream because that type lives in package
// auth_test and is not visible here; this file must stay in package auth to
// reach the unexported newJWKSAuthenticator constructor.
type localStream struct {
	grpc.ServerStream
	ctx context.Context
}

func TestJWKSAuthenticate(t *testing.T) {
	t.Parallel()

	key, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)

	goodKeyfunc := func(*jwt.Token) (any, error) { return &key.PublicKey, nil }

	sign := func(claims jwt.Claims) string {
		tok := jwt.NewWithClaims(jwt.SigningMethodRS256, claims)
		s, signErr := tok.SignedString(key)
		require.NoError(t, signErr)
		return s
	}

	bearer := func(tok string) metadata.MD { return metadata.Pairs("authorization", "Bearer "+tok) }

	t.Run("valid token", func(t *testing.T) {
		t.Parallel()
		a := newJWKSAuthenticator(goodKeyfunc, []string{"proxy"}, "https://issuer", "", "")
		tok := sign(jwt.RegisteredClaims{
			Issuer:    "https://issuer",
			Audience:  jwt.ClaimStrings{"proxy"},
			ExpiresAt: jwt.NewNumericDate(time.Now().Add(time.Hour)),
		})
		require.NoError(t, a.Authenticate(t.Context(), bearer(tok)))
	})

	t.Run("expired token", func(t *testing.T) {
		t.Parallel()
		a := newJWKSAuthenticator(goodKeyfunc, nil, "", "", "")
		tok := sign(jwt.RegisteredClaims{ExpiresAt: jwt.NewNumericDate(time.Now().Add(-time.Hour))})
		require.Equal(t, codes.Unauthenticated, status.Code(a.Authenticate(t.Context(), bearer(tok))))
	})

	t.Run("wrong audience", func(t *testing.T) {
		t.Parallel()
		a := newJWKSAuthenticator(goodKeyfunc, []string{"proxy"}, "", "", "")
		tok := sign(jwt.RegisteredClaims{
			Audience:  jwt.ClaimStrings{"other"},
			ExpiresAt: jwt.NewNumericDate(time.Now().Add(time.Hour)),
		})
		require.Equal(t, codes.Unauthenticated, status.Code(a.Authenticate(t.Context(), bearer(tok))))
	})

	t.Run("audience OR-intersection", func(t *testing.T) {
		t.Parallel()

		tests := []struct {
			name      string
			audiences []string
			tokenAud  jwt.ClaimStrings
			wantOK    bool
		}{
			{
				name:      "multi-element configured audiences, single overlap",
				audiences: []string{"other", "proxy", "third"},
				tokenAud:  jwt.ClaimStrings{"proxy"},
				wantOK:    true,
			},
			{
				name:      "multi-element token aud, single overlap",
				audiences: []string{"proxy"},
				tokenAud:  jwt.ClaimStrings{"other", "proxy", "third"},
				wantOK:    true,
			},
			{
				name:      "multi-element on both sides, overlap in middle",
				audiences: []string{"a", "proxy", "c"},
				tokenAud:  jwt.ClaimStrings{"x", "proxy", "z"},
				wantOK:    true,
			},
			{
				name:      "multi-element on both sides, no overlap",
				audiences: []string{"a", "b", "c"},
				tokenAud:  jwt.ClaimStrings{"x", "y", "z"},
				wantOK:    false,
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()
				a := newJWKSAuthenticator(goodKeyfunc, tt.audiences, "", "", "")
				tok := sign(jwt.RegisteredClaims{
					Audience:  tt.tokenAud,
					ExpiresAt: jwt.NewNumericDate(time.Now().Add(time.Hour)),
				})

				err := a.Authenticate(t.Context(), bearer(tok))
				if tt.wantOK {
					require.NoError(t, err)
					return
				}

				require.Equal(t, codes.Unauthenticated, status.Code(err))
			})
		}
	})

	t.Run("wrong issuer", func(t *testing.T) {
		t.Parallel()
		a := newJWKSAuthenticator(goodKeyfunc, nil, "https://issuer", "", "")
		tok := sign(jwt.RegisteredClaims{
			Issuer:    "https://evil",
			ExpiresAt: jwt.NewNumericDate(time.Now().Add(time.Hour)),
		})
		require.Equal(t, codes.Unauthenticated, status.Code(a.Authenticate(t.Context(), bearer(tok))))
	})

	t.Run("bad signature", func(t *testing.T) {
		t.Parallel()
		other, _ := rsa.GenerateKey(rand.Reader, 2048)
		a := newJWKSAuthenticator(func(*jwt.Token) (any, error) { return &other.PublicKey, nil }, nil, "", "", "")
		tok := sign(jwt.RegisteredClaims{ExpiresAt: jwt.NewNumericDate(time.Now().Add(time.Hour))})
		require.Equal(t, codes.Unauthenticated, status.Code(a.Authenticate(t.Context(), bearer(tok))))
	})

	t.Run("missing header", func(t *testing.T) {
		t.Parallel()
		a := newJWKSAuthenticator(goodKeyfunc, nil, "", "", "")
		require.Equal(t, codes.Unauthenticated, status.Code(a.Authenticate(t.Context(), metadata.MD{})))
	})

	t.Run("keys unavailable maps to Unavailable", func(t *testing.T) {
		t.Parallel()
		a := newJWKSAuthenticator(func(*jwt.Token) (any, error) {
			return nil, errKeysUnavailable
		}, nil, "", "", "")
		tok := sign(jwt.RegisteredClaims{ExpiresAt: jwt.NewNumericDate(time.Now().Add(time.Hour))})
		require.Equal(t, codes.Unavailable, status.Code(a.Authenticate(t.Context(), bearer(tok))))
	})

	t.Run("readiness gate reports Unavailable until the loader publishes a keyfunc", func(t *testing.T) {
		t.Parallel()

		release := make(chan struct{})
		keyfn, ready := deferredKeyfunc(func() (jwt.Keyfunc, error) {
			<-release
			return goodKeyfunc, nil
		})

		a := newJWKSAuthenticator(keyfn, nil, "", "", "")
		tok := sign(jwt.RegisteredClaims{ExpiresAt: jwt.NewNumericDate(time.Now().Add(time.Hour))})

		// The loader is still blocked on release: the gate must be closed, so
		// a well-formed token still fails with Unavailable rather than
		// blocking the caller or accepting an unverifiable token.
		require.Equal(t, codes.Unavailable, status.Code(a.Authenticate(t.Context(), bearer(tok))))
		require.Nil(t, ready.Load())

		close(release)

		// No sleeps: poll the local atomic gate (no network involved) until
		// the background goroutine has published its result.
		require.Eventually(t, func() bool {
			return ready.Load() != nil
		}, time.Second, time.Millisecond, "loader should have published a keyfunc")

		require.NoError(t, a.Authenticate(t.Context(), bearer(tok)))
	})
}

func TestNewJWKSAuthenticatorInvalidURL(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		url     string
		wantErr string
	}{
		{name: "empty", url: "", wantErr: "jwks url is required"},
		{name: "missing scheme", url: "issuer.example.com/jwks.json", wantErr: "must be a valid absolute URL"},
		{name: "missing host", url: "https:///jwks.json", wantErr: "must be a valid absolute URL"},
		{name: "unparsable", url: "https://exa\x00mple.com", wantErr: "invalid jwks url"},
		{name: "http scheme", url: "http://issuer.example.com/jwks.json", wantErr: "must use https"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			a, err := NewJWKSAuthenticator(tt.url, nil, "", "", "")
			require.Nil(t, a)
			require.ErrorContains(t, err, tt.wantErr)
		})
	}
}

// TestJWKSRejectionLogExcludesToken guards the JWKS path's server-side logging
// added for rejection reasons: the log must carry the underlying
// golang-jwt error detail (so operators can diagnose failures) but must never
// leak the raw token value, since that's a credential.
func TestJWKSRejectionLogExcludesToken(t *testing.T) {
	t.Parallel()

	key, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)

	keyfn := func(*jwt.Token) (any, error) { return &key.PublicKey, nil }
	a := newJWKSAuthenticator(keyfn, nil, "", "", "")

	expired := jwt.NewWithClaims(jwt.SigningMethodRS256, jwt.RegisteredClaims{
		ExpiresAt: jwt.NewNumericDate(time.Now().Add(-time.Hour)),
	})
	signedToken, err := expired.SignedString(key)
	require.NoError(t, err)

	var buf bytes.Buffer
	log := logger.NewZeroLogger(&buf, logger.LevelInfo)

	ic := StreamServerInterceptor(a, log)
	ctx := metadata.NewIncomingContext(t.Context(), metadata.Pairs("authorization", "Bearer "+signedToken))

	called := false
	err = ic(
		nil,
		&localStream{ctx: ctx},
		&grpc.StreamServerInfo{FullMethod: "/pkg.Svc/Method"},
		func(any, grpc.ServerStream) error {
			called = true
			return nil
		},
	)

	require.False(t, called)

	// Client sees a generic message + correct code.
	st := status.Convert(err)
	require.Equal(t, codes.Unauthenticated, st.Code())
	require.Equal(t, "invalid credentials", st.Message())

	// Server log carries the detailed reason...
	logged := buf.String()
	require.Contains(t, logged, "token verification failed")

	// ...but never the raw token value.
	require.NotContains(t, logged, signedToken)
}

func TestWrapKeyfunc(t *testing.T) {
	t.Parallel()

	notFound := fmt.Errorf("lookup: %w", jwkset.ErrKeyNotFound)

	tests := []struct {
		name        string
		resolveKey  any
		resolveErr  error
		keysPresent bool
		wantErrIs   error // nil, errKeysUnavailable, or jwkset.ErrKeyNotFound
	}{
		{"success returns key", "the-key", nil, true, nil},
		{"unknown kid, empty keyset -> unavailable", nil, notFound, false, errKeysUnavailable},
		{"unknown kid, populated keyset -> not found", nil, notFound, true, jwkset.ErrKeyNotFound},
		{"other resolver error -> unavailable", nil, errors.New("boom"), true, errKeysUnavailable},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			wf := wrapKeyfunc(
				func(*jwt.Token) (any, error) { return tt.resolveKey, tt.resolveErr },
				func() bool { return tt.keysPresent },
			)

			got, err := wf(&jwt.Token{})
			if tt.wantErrIs == nil {
				require.NoError(t, err)
				require.Equal(t, tt.resolveKey, got)
				return
			}

			require.ErrorIs(t, err, tt.wantErrIs)
			require.Nil(t, got)
		})
	}
}

func (f *localStream) Context() context.Context { return f.ctx }
