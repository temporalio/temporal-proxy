// Command starter starts one GreetingWorkflow through the proxy and prints the
// result. The workflow id is fixed so the README can point the temporal CLI at
// the same execution.
package main

import (
	"context"
	"fmt"
	"log"

	"go.temporal.io/sdk/client"

	"github.com/temporalio/temporal-proxy/examples/kms"
)

// workflowID is fixed so the README's "temporal workflow show" commands can name
// it without copying an id out of the log.
const workflowID = "kms-example-greeting"

func main() {
	c, err := client.Dial(client.Options{
		HostPort:  kms.ProxyHostPort,
		Namespace: kms.Namespace,
	})
	if err != nil {
		log.Fatalf("dial proxy: %v", err)
	}
	defer c.Close()

	run, err := c.ExecuteWorkflow(context.Background(), client.StartWorkflowOptions{
		ID:        workflowID,
		TaskQueue: kms.TaskQueue,
	}, kms.GreetingWorkflow, "Temporal")
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
