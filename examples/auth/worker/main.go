// Command worker runs a Temporal worker that authenticates to the proxy's
// gateway with a JWT from the example's identity provider.
//
// Every poll is a new stream, so every poll is authenticated: the proxy asks the
// extension server about each one. Nothing here caches a verdict, and neither
// does the proxy, which is worth knowing before pointing one of these at a slow
// identity backend.
package main

import (
	"log"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"

	"github.com/temporalio/temporal-proxy/examples/auth"
)

func main() {
	c, err := client.Dial(client.Options{
		HostPort:    auth.ProxyHostPort,
		Namespace:   auth.Namespace,
		Credentials: auth.Credentials("worker"),

		// The gateway is plaintext on loopback. Without this the SDK enables TLS
		// on its own as soon as credentials are present, and the handshake fails
		// against a plaintext listener.
		ConnectionOptions: client.ConnectionOptions{TLSDisabled: true},
	})
	if err != nil {
		log.Fatalf("dial proxy: %v", err)
	}
	defer c.Close()

	w := worker.New(c, auth.TaskQueue, worker.Options{})
	w.RegisterWorkflow(auth.GreetingWorkflow)
	w.RegisterActivity(auth.ComposeGreeting)

	log.Printf("worker listening on task queue %q (namespace %q, tenant %q)",
		auth.TaskQueue, auth.Namespace, auth.Tenant())

	if err := w.Run(worker.InterruptCh()); err != nil {
		log.Fatalf("run worker: %v", err)
	}
}
