// Command starter starts one GreetingWorkflow through the proxy and prints the
// result. It authenticates with a JWT from the example's identity provider; the
// workflow id is fixed so the README can point the temporal CLI at the same
// execution.
package main

import (
	"context"
	"fmt"
	"log"

	"go.temporal.io/sdk/client"

	"github.com/temporalio/temporal-proxy/examples/auth"
)

// workflowID is fixed so the README's "temporal workflow" commands can name it
// without copying an id out of the log.
const workflowID = "auth-example-greeting"

func main() {
	c, err := client.Dial(client.Options{
		HostPort:    auth.ProxyHostPort,
		Namespace:   auth.Namespace,
		Credentials: auth.Credentials("starter"),

		// The gateway is plaintext on loopback. Without this the SDK enables TLS
		// on its own as soon as credentials are present, and the handshake fails
		// against a plaintext listener.
		ConnectionOptions: client.ConnectionOptions{TLSDisabled: true},
	})
	if err != nil {
		log.Fatalf("dial proxy: %v", err)
	}
	defer c.Close()

	log.Printf("starting workflow as tenant %q", auth.Tenant())

	run, err := c.ExecuteWorkflow(context.Background(), client.StartWorkflowOptions{
		ID:        workflowID,
		TaskQueue: auth.TaskQueue,
	}, auth.GreetingWorkflow, "Temporal")
	if err != nil {
		log.Fatalf("start workflow: %v", err)
	}
	log.Printf("started workflow id=%s runID=%s", run.GetID(), run.GetRunID())

	var greeting string
	if err := run.Get(context.Background(), &greeting); err != nil {
		log.Fatalf("get result: %v", err)
	}

	fmt.Println(greeting)
}
