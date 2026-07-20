package auth

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"slices"
	"sync/atomic"
	"time"

	"github.com/MicahParks/jwkset"
	"github.com/MicahParks/keyfunc/v3"
	"github.com/golang-jwt/jwt/v5"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
)

const (
	// jwksHTTPTimeout bounds each JWKS fetch so a hung IdP connection cannot
	// stall an on-demand refresh (which runs synchronously on the auth path).
	jwksHTTPTimeout = 10 * time.Second

	// jwksRateLimitWaitMax bounds how long an unknown-kid lookup waits on the
	// on-demand-refresh rate limiter before giving up, so a burst of unknown
	// kids during an outage fails fast (fail-closed) instead of blocking the
	// auth path for up to keyfunc's 1m default.
	jwksRateLimitWaitMax = 5 * time.Second

	// jwksRefreshInterval is the background JWKS refresh cadence, tighter than
	// keyfunc's 1h default so a revoked key stops verifying sooner.
	jwksRefreshInterval = 15 * time.Minute
)

var (
	// validSigningMethods restricts accepted JWT algorithms to asymmetric families,
	// preventing algorithm-confusion (e.g. "none" or HMAC) attacks.
	validSigningMethods = []string{"RS256", "RS384", "RS512", "ES256", "ES384", "ES512", "PS256", "PS384", "PS512"}

	// errKeysUnavailable signals the JWKS keyset could not be consulted (not yet
	// loaded, or the IdP is unreachable), as distinct from a token that failed
	// verification. It maps to codes.Unavailable.
	errKeysUnavailable = errors.New("jwks keyset unavailable")
)

// JWKSAuthenticator verifies an inbound JWT's signature against a JWKS keyset
// and checks its standard claims.
type JWKSAuthenticator struct {
	keyfn     jwt.Keyfunc
	audiences []string
	issuer    string
	header    string
	scheme    string
}

// NewJWKSAuthenticator builds a JWKSAuthenticator that resolves signing keys
// from the JWKS at rawURL.
//
// keyfunc.NewDefaultOverrideCtx performs its first key fetch synchronously
// (see github.com/MicahParks/jwkset's NewStorageFromHTTP), so calling it
// directly here would block construction for up to its HTTP timeout if the
// IdP is unreachable. To keep construction non-blocking, the fetch is kicked
// off in a background goroutine (via deferredKeyfunc); until it completes,
// Authenticate reports codes.Unavailable (fail closed, retryable) instead of
// blocking startup.
//
// rawURL is validated synchronously before the goroutine is started, so a
// malformed configuration (bad scheme/host, not a transient IdP outage) fails
// construction immediately instead of surfacing as a permanent
// codes.Unavailable for every future request.
func NewJWKSAuthenticator(rawURL string, audiences []string, issuer, header, scheme string) (*JWKSAuthenticator, error) {
	if err := validateJWKSURL(rawURL); err != nil {
		return nil, err
	}

	keyfn, _ := deferredKeyfunc(func() (jwt.Keyfunc, error) {
		kf, err := keyfunc.NewDefaultOverrideCtx(context.Background(), []string{rawURL}, keyfunc.Override{
			HTTPTimeout:      jwksHTTPTimeout,
			RateLimitWaitMax: jwksRateLimitWaitMax,
			RefreshInterval:  jwksRefreshInterval,
		})
		if err != nil {
			return nil, err
		}

		return wrapKeyfunc(kf.Keyfunc, func() bool {
			keys, kerr := kf.Storage().KeyReadAll(context.Background())
			return kerr == nil && len(keys) > 0
		}), nil
	})

	return newJWKSAuthenticator(keyfn, audiences, issuer, header, scheme), nil
}

func newJWKSAuthenticator(keyfn jwt.Keyfunc, audiences []string, issuer, header, scheme string) *JWKSAuthenticator {
	if header == "" {
		header = defaultHeader
	}
	if scheme == "" {
		scheme = defaultScheme
	}

	return &JWKSAuthenticator{
		keyfn:     keyfn,
		audiences: audiences,
		issuer:    issuer,
		header:    header,
		scheme:    scheme,
	}
}

// Authenticate verifies the JWT carried in md: signature via the JWKS keyset,
// expiry, and (when configured) issuer and audience.
func (a *JWKSAuthenticator) Authenticate(_ context.Context, md metadata.MD) error {
	raw, ok := extractToken(md, a.header, a.scheme)
	if !ok {
		return reject(codes.Unauthenticated, "missing or malformed credentials",
			"jwks: missing or malformed "+a.header+" header")
	}

	opts := []jwt.ParserOption{
		jwt.WithValidMethods(validSigningMethods),
		jwt.WithExpirationRequired(),
	}
	if a.issuer != "" {
		opts = append(opts, jwt.WithIssuer(a.issuer))
	}

	claims := jwt.MapClaims{}
	if _, err := jwt.ParseWithClaims(raw, claims, a.keyfn, opts...); err != nil {
		if errors.Is(err, errKeysUnavailable) {
			return reject(codes.Unavailable, "authentication temporarily unavailable", "jwks: "+err.Error())
		}

		return reject(codes.Unauthenticated, "invalid credentials", "jwks: token verification failed: "+err.Error())
	}

	if len(a.audiences) > 0 && !audienceMatches(claims, a.audiences) {
		return reject(codes.Unauthenticated, "invalid credentials", "jwks: audience not allowed")
	}

	return nil
}

// audienceMatches reports whether the token's aud claim intersects allowed.
func audienceMatches(claims jwt.MapClaims, allowed []string) bool {
	aud, err := claims.GetAudience()
	if err != nil {
		return false
	}

	for _, got := range aud {
		if slices.Contains(allowed, got) {
			return true
		}
	}

	return false
}

// validateJWKSURL requires rawURL to be a syntactically valid absolute https
// URL (non-empty host, https scheme), so that a malformed JWKS endpoint fails
// construction synchronously instead of surfacing later as a permanent,
// silent codes.Unavailable outage indistinguishable from a transient IdP
// failure. https is required because the JWKS endpoint is the trust root for
// JWT verification: an http endpoint would let a network attacker substitute
// keys and forge tokens.
func validateJWKSURL(rawURL string) error {
	if rawURL == "" {
		return errors.New("auth: jwks url is required")
	}

	u, err := url.Parse(rawURL)
	if err != nil {
		return fmt.Errorf("auth: invalid jwks url %q: %w", rawURL, err)
	}

	if u.Host == "" {
		return fmt.Errorf("auth: invalid jwks url %q: must be a valid absolute URL", rawURL)
	}

	if u.Scheme != "https" {
		return fmt.Errorf("auth: invalid jwks url %q: must use https", rawURL)
	}

	return nil
}

// deferredKeyfunc runs load in a background goroutine and returns a
// jwt.Keyfunc that reports errKeysUnavailable until load completes, then
// delegates to the jwt.Keyfunc it produced. The returned *atomic.Pointer
// becomes non-nil exactly when load has completed successfully; tests can
// poll it directly (or gate load on a channel they control) to exercise the
// readiness gate deterministically, without relying on real network timing.
func deferredKeyfunc(load func() (jwt.Keyfunc, error)) (jwt.Keyfunc, *atomic.Pointer[jwt.Keyfunc]) {
	var ready atomic.Pointer[jwt.Keyfunc]

	go func() {
		kf, err := load()
		if err != nil {
			// If load fails, ready stays nil permanently; Authenticate returns
			// codes.Unavailable indefinitely (not retried). Expected unreachable:
			// URL validation is synchronous, and keyfunc's HTTP storage (used by
			// keyfunc.NewDefaultOverrideCtx) swallows the first fetch failure
			// rather than erroring.
			return
		}

		ready.Store(&kf)
	}()

	keyfn := func(t *jwt.Token) (any, error) {
		kf := ready.Load()
		if kf == nil {
			return nil, errKeysUnavailable
		}

		return (*kf)(t)
	}

	return keyfn, &ready
}

// wrapKeyfunc adapts a key resolver so the error taxonomy matches the spec's
// intent: a genuinely unknown key id on a POPULATED keyset is a verification
// failure (jwkset.ErrKeyNotFound -> codes.Unauthenticated), while an unknown
// key id on an EMPTY keyset means the keyset was never fetched (IdP unreachable
// at startup and on-demand refresh still failing), which is an availability
// problem (errKeysUnavailable -> codes.Unavailable), not a bad token. Any other
// resolver error is treated as an availability problem.
//
// keysPresent reports whether the keyset currently holds any keys. resolve is
// the underlying key resolver (in production, keyfunc.Keyfunc's method value).
//
// When keysPresent cannot positively confirm keys exist (e.g. its underlying
// read errors), it should return false; the failure then maps to
// codes.Unavailable (retryable) rather than masking as a bad token - the
// conservative fail-closed-toward-retryable choice for an infra-level read
// error.
func wrapKeyfunc(resolve jwt.Keyfunc, keysPresent func() bool) jwt.Keyfunc {
	return func(t *jwt.Token) (any, error) {
		key, err := resolve(t)
		if err == nil {
			return key, nil
		}

		if errors.Is(err, jwkset.ErrKeyNotFound) {
			if !keysPresent() {
				return nil, fmt.Errorf("%w: empty keyset", errKeysUnavailable)
			}

			return nil, err
		}

		return nil, fmt.Errorf("%w: %v", errKeysUnavailable, err)
	}
}
