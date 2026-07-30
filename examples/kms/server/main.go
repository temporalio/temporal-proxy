// Command server is the example's extension server: a gRPC service implementing
// api.kms.v1.EncryptionService that wraps and unwraps the proxy's data encryption
// keys. Only key material crosses the wire; payload plaintext never reaches it.
//
// It reads two secrets from the environment. KMS_API_KEY is the bearer token
// every call must present, matching the proxy's credentials block.
// KMS_MASTER_SECRET is the secret every namespace's wrapping key is derived
// from. Both are required.
//
// The provider it serves lives alongside it: keyring.go derives one AES-256-GCM
// key per namespace from the master secret and frames the ciphertext, service.go
// is the gRPC surface, and interceptor.go is the bearer token check. That is
// enough to show the shape of the contract, and it is not a key manager: the
// master secret sits in an environment variable, nothing is rotated, and losing
// the secret loses every payload sealed under it. A real provider fronts an HSM
// or a key service.
package main

import (
	"crypto/tls"
	"flag"
	"log"
	"net"
	"os"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	"github.com/temporalio/temporal-proxy/pkg/api/kms/v1"
)

func main() {
	listen := flag.String("listen", "127.0.0.1:9443", "address to serve on")
	certFile := flag.String("cert", "certs/server.pem", "PEM server certificate")
	keyFile := flag.String("key", "certs/server-key.pem", "PEM private key matching -cert")
	flag.Parse()

	// Fail loudly on a missing secret. A provider that started without a token
	// would serve anyone who found the port, and one without a master secret has
	// no key to wrap with.
	token := requireEnv("KMS_API_KEY")
	secret := requireEnv("KMS_MASTER_SECRET")

	keys, err := newKeyring([]byte(secret))
	if err != nil {
		log.Fatalf("failed to build keyring: %v", err)
	}

	cert, err := tls.LoadX509KeyPair(*certFile, *keyFile)
	if err != nil {
		log.Fatalf("failed to load key pair: %v (generate one with: go run ./gencerts)", err)
	}

	lis, err := net.Listen("tcp", *listen)
	if err != nil {
		log.Fatalf("failed to listen on %s: %v", *listen, err)
	}

	// TLS 1.2 is the floor the proxy's dialer enforces; two Go peers negotiate
	// 1.3 in practice.
	creds := credentials.NewTLS(&tls.Config{
		Certificates: []tls.Certificate{cert},
		MinVersion:   tls.VersionTLS12,
	})

	srv := grpc.NewServer(
		grpc.Creds(creds),
		grpc.UnaryInterceptor(authInterceptor(token)),
	)
	kms.RegisterEncryptionServiceServer(srv, newService(keys))

	log.Printf("extension server listening on %s over TLS", *listen)
	if err := srv.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}

func requireEnv(name string) string {
	v := os.Getenv(name)
	if v == "" {
		log.Fatalf("%s is required", name)
	}

	return v
}
