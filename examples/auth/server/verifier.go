package main

import (
	"context"
	"errors"
	"slices"
	"strings"

	"github.com/MicahParks/jwkset"
	"github.com/golang-jwt/jwt/v5"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/temporalio/temporal-proxy/pkg/api/auth/v1"
	"github.com/temporalio/temporal-proxy/pkg/logger"
	"github.com/temporalio/temporal-proxy/pkg/logger/tag"
)

const (
	// credentialHeader is the metadata key the proxy lifts the caller's
	// credential from. It has to match config.yaml's
	// auth.external.credentialHeaders: a verdict reports only admit or deny, so
	// nothing in the exchange would reveal a disagreement.
	credentialHeader = "authorization"

	// bearerPrefix is the scheme the Temporal SDK's API key credentials prepend.
	bearerPrefix = "Bearer "
)

// verifier admits a caller whose JWT verifies against the identity provider's
// JWKS and whose tenant claim names a permitted tenant.
type verifier struct {
	keyfn    jwt.Keyfunc
	issuer   string
	audience string
	tenants  []string
	log      logger.Logger
}

// Authenticate implements ext.Auth.
//
// The two failure codes are the interesting part, and they are not
// interchangeable. A credential problem is Unauthenticated: the caller can fix
// it by getting a new token. A valid token from a tenant this server does not
// serve is PermissionDenied: the credential is perfectly good and a fresh one
// changes nothing. Collapsing them into one code sends every rejected caller
// into a token-refresh loop that cannot succeed.
//
// The proxy keeps the code and discards the message (see internal/api/auth.go),
// so these messages are written for whoever operates this server. The reason a
// caller was turned away lives in this log and nowhere else.
func (v *verifier) Authenticate(_ context.Context, creds []*auth.CallerCredential) error {
	raw, err := bearerToken(creds)
	if err != nil {
		v.log.Info("Rejected a credential", tag.Error(err))

		return err
	}

	claims := jwt.MapClaims{}
	if _, err := jwt.ParseWithClaims(raw, claims, v.keyfn,
		// Pinning the algorithm is what stops an attacker picking a weaker one, or
		// "none", on our behalf. The issuer and audience are required to be non-empty
		// where they are configured, because the library skips a claim it has no
		// value to compare against.
		jwt.WithValidMethods([]string{"ES256"}),
		jwt.WithExpirationRequired(),
		jwt.WithIssuer(v.issuer),
		jwt.WithAudience(v.audience),
	); err != nil {
		if keysUnavailable(err) {
			v.log.Error("Cannot consult the JWKS", tag.Error(err))

			return status.Error(codes.Unavailable, "jwks unavailable: "+err.Error())
		}

		v.log.Info("Rejected a token", tag.Error(err))

		return status.Error(codes.Unauthenticated, "token verification failed: "+err.Error())
	}

	// A missing claim, or one that is not a string, yields "", which no entry in
	// the allowlist can match because splitTenants drops blanks.
	sub, _ := claims["sub"].(string)
	tenant, _ := claims["tenant"].(string)

	if !slices.Contains(v.tenants, tenant) {
		v.log.Info("Refused a tenant", tag.String("sub", sub), tag.String("tenant", tenant))

		return status.Error(codes.PermissionDenied, "tenant not permitted: "+tenant)
	}

	v.log.Info("Admitted a caller", tag.String("sub", sub), tag.String("tenant", tenant))

	return nil
}

// bearerToken pulls the single bearer token out of the credentials the proxy
// lifted from the caller's stream. An empty slice means the caller presented
// none of the declared headers, which is an unauthenticated caller rather than
// a trusted one.
func bearerToken(creds []*auth.CallerCredential) (string, error) {
	idx := slices.IndexFunc(creds, func(c *auth.CallerCredential) bool {
		return c.GetHeader() == credentialHeader
	})
	if idx < 0 {
		return "", status.Error(codes.Unauthenticated, "no "+credentialHeader+" credential")
	}

	vals := creds[idx].GetValues()
	if len(vals) != 1 {
		// Refused rather than searched: gRPC metadata lets a key repeat, and
		// trying each value would let a caller spray guesses in a single call.
		return "", status.Error(codes.Unauthenticated, credentialHeader+" must carry exactly one value")
	}

	if !strings.HasPrefix(vals[0], bearerPrefix) {
		return "", status.Error(codes.Unauthenticated, credentialHeader+" is not a bearer token")
	}

	return strings.TrimPrefix(vals[0], bearerPrefix), nil
}

// keysUnavailable reports whether err means the keyset could not be consulted,
// as opposed to the token being bad. Only key resolution can raise that
// question, and jwt reports every resolution failure as ErrTokenUnverifiable.
//
// Within those, a key id the keyset simply does not have is a bad token: it was
// signed by something this server does not trust, which is what a rotated
// identity provider key looks like. Anything else is an infrastructure problem
// and gets the retryable code, because a caller holding a perfectly good token
// should not be told to go and get another one.
//
// The proxy's built-in authenticator needs a third case here, an empty keyset
// meaning the very first fetch never landed. This server does not, because it
// fetches synchronously at startup and refuses to run without a keyset.
func keysUnavailable(err error) bool {
	if !errors.Is(err, jwt.ErrTokenUnverifiable) {
		return false
	}

	return !errors.Is(err, jwkset.ErrKeyNotFound)
}
