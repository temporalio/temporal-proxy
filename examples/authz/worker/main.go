// Command worker runs a Temporal worker that connects to the proxy's gateway at
// 127.0.0.1:7234 and serves the authz-example task queue. It presents the token
// in AUTHZ_TOKEN, which the proxy hands to the extension server for a verdict on
// every stream, including every poll.
//
// AUTHZ_TOKEN may be left unset, which dials with no credential at all. That is
// one of the cases the README walks through: the connection still succeeds,
// because GetSystemInfo is allowed to everyone, and the first poll is refused.
package main

import (
	"log"
	"os"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"

	"github.com/temporalio/temporal-proxy/examples/authz"
)

const (
	gateway   = "127.0.0.1:7234"
	namespace = "default"
)

func main() {
	opts := client.Options{
		HostPort:  gateway,
		Namespace: namespace,
		// Load bearing: an API key credential turns TLS on unless TLS is explicitly
		// disabled, and this gateway is plaintext on loopback. Without this the SDK
		// opens a TLS handshake the proxy cannot answer.
		ConnectionOptions: client.ConnectionOptions{TLSDisabled: true},
	}

	if token := os.Getenv("AUTHZ_TOKEN"); token != "" {
		// Sent as "authorization: Bearer <token>", the header config.yaml declares.
		opts.Credentials = client.NewAPIKeyStaticCredentials(token)
	} else {
		log.Print("AUTHZ_TOKEN is unset: dialing with no credential")
	}

	c, err := client.Dial(opts)
	if err != nil {
		log.Fatalf("dial proxy: %v", err)
	}
	defer c.Close()

	w := worker.New(c, authz.TaskQueue, worker.Options{})
	w.RegisterWorkflow(authz.GreetingWorkflow)
	w.RegisterActivity(authz.ComposeGreeting)

	log.Printf("worker listening on task queue %q (namespace %q)", authz.TaskQueue, namespace)
	if err := w.Run(worker.InterruptCh()); err != nil {
		log.Fatalf("run worker: %v", err)
	}
}
