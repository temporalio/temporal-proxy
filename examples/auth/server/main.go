// Command server is the example's extension server: it answers
// api.auth.v1.AuthService, deciding whether an inbound caller of the proxy may
// proceed.
//
// The decision has two halves. The first is ordinary JWT verification against
// the JWKS the identity provider publishes, which the proxy's built-in "jwks"
// authenticator does just as well and with less of your code to maintain. The
// second is why this server exists: the token's tenant claim has to name a
// tenant this server serves, and that is a rule the built-in cannot express.
//
// verifier.go is the part worth reading and the part you replace. The listener,
// graceful shutdown, and service registration all come from
// [github.com/temporalio/temporal-proxy/pkg/ext].
package main

import (
	"context"
	"flag"
	"os"
	"strings"
	"time"

	"github.com/MicahParks/keyfunc/v3"

	"github.com/temporalio/temporal-proxy/pkg/ext"
	"github.com/temporalio/temporal-proxy/pkg/logger"
	"github.com/temporalio/temporal-proxy/pkg/logger/tag"
)

const (
	// jwksHTTPTimeout bounds each fetch, including the synchronous one at startup.
	jwksHTTPTimeout = 10 * time.Second

	// jwksRefreshInterval is the background refetch cadence, tighter than
	// keyfunc's one hour default so a rotated or revoked key stops verifying
	// sooner.
	jwksRefreshInterval = 15 * time.Minute
)

func main() {
	listen := flag.String("listen", "127.0.0.1:9500", "address to serve on")
	jwksURL := flag.String("jwks", envOr("AUTH_JWKS_URL", "http://127.0.0.1:9080/.well-known/jwks.json"),
		"JWKS to verify inbound tokens against")
	issuer := flag.String("issuer", envOr("AUTH_ISSUER", "http://127.0.0.1:9080/"), "required iss claim")
	audience := flag.String("audience", envOr("AUTH_AUDIENCE", "temporal-proxy"), "required aud claim")
	tenants := flag.String("tenants", envOr("AUTH_TENANTS", "acme,globex"),
		"comma separated tenant claims to admit")
	flag.Parse()

	log := logger.Default().With(tag.Component("examples"))

	allowed := splitTenants(*tenants)
	if len(allowed) == 0 {
		// An empty allowlist would deny every caller while looking like a working
		// server, so it is a configuration error rather than a default.
		log.Fatal("At least one permitted tenant is required")
	}

	// Neither of these may be empty. An empty expected issuer or audience does not
	// loosen the check, it removes it: the JWT library only verifies a claim it was
	// given a value to compare against. Refusing to start beats running a verifier
	// that silently checks less than its flags suggest.
	if *issuer == "" || *audience == "" {
		log.Fatal("An issuer and an audience are both required")
	}

	// The first fetch is synchronous and fatal. This server has nothing useful to
	// say without a keyset, and "start the idp first" beats answering Unavailable
	// to every caller until somebody reads the logs. The proxy's own built-in
	// authenticator makes the opposite call and defers this, because a slow
	// identity provider must not be able to block proxy startup.
	strict := false
	keys, err := keyfunc.NewDefaultOverrideCtx(context.Background(), []string{*jwksURL}, keyfunc.Override{
		HTTPTimeout:               jwksHTTPTimeout,
		RefreshInterval:           jwksRefreshInterval,
		NoErrorReturnFirstHTTPReq: &strict,
	})
	if err != nil {
		log.Fatal("Failed to fetch the JWKS (is the idp running?)", tag.Error(err))
	}

	log.Info("Verifying tokens",
		tag.String("jwks", *jwksURL),
		tag.String("issuer", *issuer),
		tag.String("audience", *audience),
		tag.String("tenants", strings.Join(allowed, ",")),
	)

	if err := ext.Serve(
		context.Background(),
		ext.WithAddr(*listen),
		ext.WithAuth(&verifier{
			keyfn:    keys.Keyfunc,
			issuer:   *issuer,
			audience: *audience,
			tenants:  allowed,
			log:      log,
		}),
		ext.WithLogger(log),

		// This server is unguarded: anything that can reach its port can ask it to
		// authenticate a caller. Guarding it is ext.WithServerAuth, and it is not a
		// one line change, which is why the call below stays commented out. The
		// proxy will not put a credential on a plaintext connection to an extension
		// server, so enabling this also means serving TLS here and adding tls and
		// credentials blocks to config.yaml. examples/kms does all of that.
		//
		// ext.WithServerAuth("authorization", func(tok string) bool {
		// 	want := []byte(bearerPrefix + os.Getenv("AUTH_API_KEY"))
		// 	return subtle.ConstantTimeCompare([]byte(tok), want) == 1
		// }),
	); err != nil {
		log.Fatal("Failed to start server", tag.Error(err))
	}
}

// splitTenants parses the comma separated allowlist, dropping blanks so a
// trailing comma or a stray space is not read as a tenant named "".
func splitTenants(raw string) []string {
	out := make([]string, 0, 4)
	for t := range strings.SplitSeq(raw, ",") {
		if t = strings.TrimSpace(t); t != "" {
			out = append(out, t)
		}
	}

	return out
}

func envOr(name, fallback string) string {
	if v := os.Getenv(name); v != "" {
		return v
	}

	return fallback
}
