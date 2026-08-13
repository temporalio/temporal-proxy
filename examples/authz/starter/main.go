// Command starter starts one GreetingWorkflow through the proxy's gateway at
// 127.0.0.1:7234 and prints the result. It presents the token in AUTHZ_TOKEN,
// which decides whether the proxy admits it: starting a workflow is a write, so a
// read-only token gets as far as connecting and no further.
//
// AUTHZ_TOKEN may be left unset, which dials with no credential at all. See the
// worker command, and the README, for what that does.
package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"go.temporal.io/sdk/client"

	"github.com/temporalio/temporal-proxy/examples/authz"
)

const (
	gateway    = "127.0.0.1:7234"
	namespace  = "default"
	workflowID = "authz-example-greeting"
)

func main() {
	opts := client.Options{
		HostPort:  gateway,
		Namespace: namespace,
		// Load bearing; see the same comment in the worker command.
		ConnectionOptions: client.ConnectionOptions{TLSDisabled: true},
	}

	if token := os.Getenv("AUTHZ_TOKEN"); token != "" {
		opts.Credentials = client.NewAPIKeyStaticCredentials(token)
	} else {
		log.Print("AUTHZ_TOKEN is unset: dialing with no credential")
	}

	c, err := client.Dial(opts)
	if err != nil {
		log.Fatalf("dial proxy: %v", err)
	}
	defer c.Close()

	run, err := c.ExecuteWorkflow(context.Background(), client.StartWorkflowOptions{
		ID:        workflowID,
		TaskQueue: authz.TaskQueue,
	}, authz.GreetingWorkflow, "Temporal")
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
