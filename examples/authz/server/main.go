// Command server is the example's extension server: a gRPC service implementing
// api.auth.v1.AuthService that decides whether each inbound caller of the proxy
// may proceed. It answers one RPC per stream the proxy accepts.
//
// It reads one secret from the environment. AUTHZ_JWT_SECRET is the HMAC secret
// every caller's token must be signed with, the same secret the gentoken command
// signs them with. It is required.
//
// The decision is split across two files, along the same seam Temporal draws
// between a ClaimMapper and an Authorizer. claims.go verifies the token and turns
// its permissions claim into roles, knowing nothing about what the caller is
// trying to reach. authorizer.go weighs those roles against the method and
// namespace the proxy resolved for the call. Both are worth reading if you are
// writing one of these; authorizer.go is where the policy lives.
//
// The gRPC surface, graceful shutdown, and signal handling all come from
// [github.com/temporalio/temporal-proxy/pkg/ext]: authorizer's Authenticate
// satisfies [ext.Auth], and [ext.Serve] does the rest.
//
// The proxy reaches this server in plaintext with no credential of its own, which
// keeps the example's setup at nothing and its subject on the decision. It also
// means the caller's token crosses that hop in the clear and anyone who reaches
// this port can ask it for verdicts. [ext.WithServerAuth] is how you require a
// credential here, but adding it alone is not enough: the proxy will not put a
// credential on a plaintext connection, so guarding this server also means
// serving TLS and adding tls and credentials blocks to the proxy's extension
// server config. The kms example shows that arrangement in full.
package main

import (
	"context"
	"flag"
	"os"

	"github.com/temporalio/temporal-proxy/pkg/ext"
	"github.com/temporalio/temporal-proxy/pkg/logger"
	"github.com/temporalio/temporal-proxy/pkg/logger/tag"
)

func main() {
	listen := flag.String("listen", "127.0.0.1:9444", "address to serve on")
	flag.Parse()

	log := logger.Default().With(tag.Component("examples"))

	secret := os.Getenv("AUTHZ_JWT_SECRET")
	if secret == "" {
		log.Fatal("AUTHZ_JWT_SECRET is required")
	}

	if err := ext.Serve(
		context.Background(),
		ext.WithAddr(*listen),
		ext.WithAuth(newAuthorizer(newMapper([]byte(secret), log), log)),
		ext.WithLogger(log),
	); err != nil {
		log.Fatal("Failed to start server", tag.Error(err))
	}
}
