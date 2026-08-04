// Command idp is a stand-in identity provider for the inbound authentication
// example. It generates one ES256 signing key in memory at startup, publishes
// the public half as a JWKS, and mints tokens for whoever asks.
//
// It exists so the extension server's JWKS fetch is a real HTTP fetch against a
// real endpoint, which is the half of the exchange worth seeing. It is not an
// identity provider: the key never leaves memory, so restarting this process
// invalidates every token it ever issued, and /token authenticates nobody at
// all. Anyone who can reach it can mint a token for any tenant.
package main

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

// coordLen is the width of a P-256 coordinate, which a JWK carries big-endian
// and zero-padded to that fixed length rather than minimally encoded. The
// uncompressed encoding [ecdsa.PublicKey.Bytes] returns already pads them, so
// this is only used to split it and to check its length.
const coordLen = 32

type (
	// jwk is one public key as it appears in a JWKS. Hand-written rather than
	// taken from a library so the whole document a verifier fetches is visible
	// in one place.
	jwk struct {
		Kty string `json:"kty"`
		Crv string `json:"crv"`
		X   string `json:"x"`
		Y   string `json:"y"`
		Kid string `json:"kid"`
		Alg string `json:"alg"`
		Use string `json:"use"`
	}

	// jwks is the document served at /.well-known/jwks.json.
	jwks struct {
		Keys []jwk `json:"keys"`
	}

	// idp holds the signing key and the claim values every minted token carries.
	// The JWK is built once at startup, so the key served and the key id stamped
	// on every token cannot drift apart.
	idp struct {
		key      *ecdsa.PrivateKey
		jwk      jwk
		issuer   string
		audience string
		ttl      time.Duration
	}
)

func main() {
	// The issuer and audience default from the same environment variables the
	// extension server reads, so setting one of them configures both ends. Setting
	// it for only the verifier would leave every caller refused for a claim
	// mismatch neither side reports in full.
	listen := flag.String("listen", "127.0.0.1:9080", "address to serve on")
	issuer := flag.String("issuer", envOr("AUTH_ISSUER", "http://127.0.0.1:9080/"), "iss claim to mint")
	audience := flag.String("audience", envOr("AUTH_AUDIENCE", "temporal-proxy"), "aud claim to mint")
	ttl := flag.Duration("ttl", 15*time.Minute, "default token lifetime")
	flag.Parse()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		log.Fatalf("generate signing key: %v", err)
	}

	pub, err := newJWK(&key.PublicKey)
	if err != nil {
		log.Fatalf("encode signing key: %v", err)
	}

	p := &idp{
		key:      key,
		jwk:      pub,
		issuer:   *issuer,
		audience: *audience,
		ttl:      *ttl,
	}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /.well-known/jwks.json", p.serveJWKS)
	mux.HandleFunc("GET /token", p.serveToken)

	svr := &http.Server{
		Addr:              *listen,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	log.Printf("idp listening on %s (kid=%s iss=%s aud=%s)", *listen, p.jwk.Kid, p.issuer, p.audience)
	if err := svr.ListenAndServe(); err != nil {
		log.Fatalf("serve: %v", err)
	}
}

// serveJWKS publishes the public key. Every request is logged because the count
// is the interesting part: a run that authenticates dozens of streams should
// fetch this once, which is the verifier's cache doing its job.
func (p *idp) serveJWKS(w http.ResponseWriter, r *http.Request) {
	log.Printf("jwks fetched by %s", r.RemoteAddr)

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(jwks{Keys: []jwk{p.jwk}}); err != nil {
		log.Printf("write jwks: %v", err)
	}
}

// serveToken mints a token. The tenant, subject, and lifetime all come straight
// off the query string with nothing authenticating the request, which is the
// single largest way this differs from an identity provider you would deploy.
func (p *idp) serveToken(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	tenant := valueOr(q.Get("tenant"), "acme")
	sub := valueOr(q.Get("sub"), "worker")

	ttl := p.ttl
	if raw := q.Get("ttl"); raw != "" {
		parsed, err := time.ParseDuration(raw)
		if err != nil {
			http.Error(w, "bad ttl: "+err.Error(), http.StatusBadRequest)

			return
		}

		ttl = parsed
	}

	now := time.Now()
	tok := jwt.NewWithClaims(jwt.SigningMethodES256, jwt.MapClaims{
		"iss":    p.issuer,
		"aud":    p.audience,
		"sub":    sub,
		"iat":    now.Unix(),
		"exp":    now.Add(ttl).Unix(),
		"tenant": tenant,
	})
	tok.Header["kid"] = p.jwk.Kid

	signed, err := tok.SignedString(p.key)
	if err != nil {
		http.Error(w, "sign: "+err.Error(), http.StatusInternalServerError)

		return
	}

	log.Printf("minted token: sub=%s tenant=%s ttl=%s", sub, tenant, ttl)

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(map[string]any{
		"access_token": signed,
		"token_type":   "Bearer",
		"expires_in":   int(ttl.Seconds()),
	}); err != nil {
		log.Printf("write token: %v", err)
	}
}

// newJWK describes pub as the single key this server publishes, with a key id
// derived from the key itself.
//
// The coordinates come out of the uncompressed SEC 1 encoding, which is a 0x04
// tag followed by x and y each padded to the curve's width. Reading them from
// [ecdsa.PublicKey.X] and Y instead would mean handling [big.Int] values that
// carry no fixed width of their own, which is part of why those fields are
// deprecated.
func newJWK(pub *ecdsa.PublicKey) (jwk, error) {
	// Bytes reports an error for a key on an unsupported curve or one that is
	// simply invalid, neither of which a freshly generated P-256 key can be.
	point, err := pub.Bytes()
	if err != nil {
		return jwk{}, err
	}

	if len(point) != 1+2*coordLen {
		return jwk{}, fmt.Errorf("want a %d byte uncompressed point, got %d", 1+2*coordLen, len(point))
	}

	x := b64(point[1 : 1+coordLen])
	y := b64(point[1+coordLen:])

	return jwk{
		Kty: "EC",
		Crv: "P-256",
		X:   x,
		Y:   y,
		Kid: thumbprint(x, y),
		Alg: "ES256",
		Use: "sig",
	}, nil
}

// thumbprint derives a key id from the key itself (RFC 7638), so the value in
// the JWKS and the value in every token header cannot drift apart. The hash
// covers the required members only, lexicographically ordered with no
// whitespace, which for an EC key is crv, kty, x, y.
func thumbprint(x, y string) string {
	sum := sha256.Sum256([]byte(`{"crv":"P-256","kty":"EC","x":"` + x + `","y":"` + y + `"}`))

	return b64(sum[:])
}

func b64(b []byte) string { return base64.RawURLEncoding.EncodeToString(b) }

func envOr(name, fallback string) string {
	return valueOr(os.Getenv(name), fallback)
}

func valueOr(got, fallback string) string {
	if got == "" {
		return fallback
	}

	return got
}
