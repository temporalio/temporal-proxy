// Command server is the example's extension server: a gRPC service implementing
// api.kms.v1.EncryptionService that wraps and unwraps the proxy's data encryption
// keys. Only key material crosses the wire; payload plaintext never reaches it.
//
// It reads two secrets from the environment. KMS_API_KEY is the bearer token
// every call must present, matching the proxy's credentials block.
// KMS_MASTER_SECRET is the secret every namespace's wrapping key is derived
// from. Both are required.
//
// Only the provider itself lives here, in keyring.go, which derives one
// AES-256-GCM key per namespace from the master secret and frames the
// ciphertext. The gRPC surface and the bearer token check come from
// [github.com/temporalio/temporal-proxy/pkg/ext]: keyring's Wrap and Unwrap
// satisfy [ext.KMS], and [ext.Serve] registers them, serves TLS, and shuts down
// on a signal. That split is the point of the example. The interesting part of
// writing one of these is the key handling, not the server around it.
//
// This is enough to show the shape of the contract and it is not a key manager:
// the master secret sits in an environment variable, nothing is rotated, and
// losing the secret loses every payload sealed under it. A real provider fronts
// an HSM or a key service.
package main

import (
	"context"
	"crypto/subtle"
	"crypto/tls"
	"flag"
	"os"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	"github.com/temporalio/temporal-proxy/pkg/ext"
	"github.com/temporalio/temporal-proxy/pkg/logger"
	"github.com/temporalio/temporal-proxy/pkg/logger/tag"
)

func main() {
	listen := flag.String("listen", "127.0.0.1:9443", "address to serve on")
	certFile := flag.String("cert", "certs/server.pem", "PEM server certificate")
	keyFile := flag.String("key", "certs/server-key.pem", "PEM private key matching -cert")
	flag.Parse()

	log := logger.Default().With(tag.Component("examples"))
	secret := requireEnv("KMS_MASTER_SECRET", log)
	expToken := []byte("Bearer " + requireEnv("KMS_API_KEY", log))

	keys, err := newKeyring([]byte(secret))
	if err != nil {
		log.Fatal("Failed to build keyring", tag.Error(err))
	}

	cert, err := tls.LoadX509KeyPair(*certFile, *keyFile)
	if err != nil {
		log.Fatal("Failed to load key pair (generate one with: go run ./gencerts)", tag.Error(err))
	}

	// TLS 1.2 is the floor the proxy's dialer enforces; two Go peers negotiate
	// 1.3 in practice.
	creds := credentials.NewTLS(&tls.Config{
		Certificates: []tls.Certificate{cert},
		MinVersion:   tls.VersionTLS12,
	})

	if err := ext.Serve(
		context.Background(),
		ext.WithAddr(*listen),
		ext.WithServerAuth("authorization", func(token string) bool {
			return subtle.ConstantTimeCompare([]byte(token), expToken) == 1
		}),
		ext.WithKMS(keys),
		ext.WithLogger(log),
		ext.WithServerOption(grpc.Creds(creds)),
	); err != nil {
		log.Fatal("Failed to start server", tag.Error(err))
	}
}

func requireEnv(name string, log logger.Logger) string {
	v := os.Getenv(name)
	if v == "" {
		log.Fatal(name + " is required")
	}

	return v
}
