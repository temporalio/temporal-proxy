package auth

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"sync"
	"time"

	"go.temporal.io/sdk/client"
	"golang.org/x/sync/singleflight"
)

const (
	// refreshWindow is how far ahead of expiry a cached token is replaced. A
	// token minted with a shorter lifetime than this is refetched on every call,
	// which is what makes AUTH_TTL a usable way to watch expiry handling.
	refreshWindow = time.Minute

	// tokenHTTPTimeout bounds one mint request.
	tokenHTTPTimeout = 10 * time.Second
)

// tokenSource caches one token from the identity provider and refetches it
// before it expires.
type tokenSource struct {
	idpURL  string
	tenant  string
	subject string
	ttl     string

	// flight collapses concurrent refreshes into one request. It is what lets the
	// mutex below cover only the cached values and never a network call.
	flight singleflight.Group

	mu      sync.Mutex
	token   string
	expires time.Time
}

// Credentials returns Temporal client credentials that present a JWT from the
// example's identity provider on every request, refreshing it before it
// expires.
//
// Dynamic rather than static because a worker outlives any sane token lifetime.
// A static credential would make this example work for fifteen minutes and then
// quietly stop, which is the failure mode most worth not shipping.
//
// subject becomes the token's sub claim, which is what the extension server
// logs to say who it admitted. Callers pass their own name so the worker and the
// starter are told apart in that log rather than both arriving as the same
// subject.
//
// Read from the environment: AUTH_IDP_URL (default http://127.0.0.1:9080),
// AUTH_TENANT (default acme), and AUTH_TTL (default empty, meaning whatever the
// identity provider's own default is).
func Credentials(subject string) client.Credentials {
	src := &tokenSource{
		idpURL:  envOr("AUTH_IDP_URL", "http://127.0.0.1:9080"),
		tenant:  Tenant(),
		subject: subject,
		ttl:     os.Getenv("AUTH_TTL"),
	}

	return client.NewAPIKeyDynamicCredentials(src.get)
}

// Tenant is the tenant claim this process asks to have minted. Setting
// AUTH_TENANT to something the extension server does not serve is how the
// example demonstrates a rejection.
func Tenant() string { return envOr("AUTH_TENANT", "acme") }

// get returns a token good for at least refreshWindow, minting a new one if the
// cached one is too close to expiry.
//
// This runs on every request the client makes, and a worker has several pollers
// in flight at once, so the refresh is arranged to hold no lock while it waits
// on the identity provider. singleflight means the pollers that arrive together
// share one request instead of each starting their own, and the mutex covers only
// the cached token and its expiry.
//
// One consequence of sharing: the request runs under the context of whichever
// caller started it, so if that caller goes away the others see its error and
// retry on their next request rather than inheriting a cancelled call.
func (s *tokenSource) get(ctx context.Context) (string, error) {
	if tok, ok := s.cached(); ok {
		return tok, nil
	}

	tok, err, _ := s.flight.Do("token", func() (any, error) {
		// Checked again inside the flight: a caller that queued behind a refresh
		// which has already finished should use its result, not start another.
		if tok, ok := s.cached(); ok {
			return tok, nil
		}

		minted, exp, err := s.mint(ctx)
		if err != nil {
			// Returned rather than falling back to the stale token. A caller that
			// cannot get a credential should say so, not send one the proxy is about
			// to reject and leave the real failure to be inferred.
			return nil, err
		}

		s.mu.Lock()
		defer s.mu.Unlock()
		s.token, s.expires = minted, exp

		return minted, nil
	})
	if err != nil {
		return "", err
	}

	return tok.(string), nil
}

// cached returns the cached token and whether it is still far enough from expiry
// to use.
func (s *tokenSource) cached() (string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.token, s.token != "" && time.Until(s.expires) > refreshWindow
}

// mint asks the identity provider for a token.
func (s *tokenSource) mint(ctx context.Context) (string, time.Time, error) {
	q := url.Values{"tenant": {s.tenant}, "sub": {s.subject}}
	if s.ttl != "" {
		q.Set("ttl", s.ttl)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, s.idpURL+"/token?"+q.Encode(), nil)
	if err != nil {
		return "", time.Time{}, fmt.Errorf("build token request: %w", err)
	}

	res, err := (&http.Client{Timeout: tokenHTTPTimeout}).Do(req)
	if err != nil {
		return "", time.Time{}, fmt.Errorf("fetch token from %s (is the idp running?): %w", s.idpURL, err)
	}
	defer res.Body.Close()

	if res.StatusCode != http.StatusOK {
		return "", time.Time{}, fmt.Errorf("fetch token from %s: %s", s.idpURL, res.Status)
	}

	var body struct {
		AccessToken string `json:"access_token"`
		ExpiresIn   int    `json:"expires_in"`
	}
	if err := json.NewDecoder(res.Body).Decode(&body); err != nil {
		return "", time.Time{}, fmt.Errorf("decode token: %w", err)
	}

	if body.AccessToken == "" {
		return "", time.Time{}, fmt.Errorf("identity provider at %s returned no token", s.idpURL)
	}

	return body.AccessToken, time.Now().Add(time.Duration(body.ExpiresIn) * time.Second), nil
}

func envOr(name, fallback string) string {
	if v := os.Getenv(name); v != "" {
		return v
	}

	return fallback
}
