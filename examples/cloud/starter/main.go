// Command starter starts one GreetingWorkflow through the proxy at
// localhost:7233 and prints the result.
package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"go.temporal.io/sdk/client"

	"github.com/temporalio/temporal-proxy/examples/cloud"
)

func main() {
	c, err := client.Dial(client.Options{
		HostPort:  "localhost:7233",
		Namespace: os.Getenv("TEMPORAL_NAMESPACE"),
	})
	if err != nil {
		log.Fatalf("dial proxy: %v", err)
	}
	defer c.Close()

	run, err := c.ExecuteWorkflow(context.Background(), client.StartWorkflowOptions{
		ID:        "cloud-example-greeting",
		TaskQueue: cloud.TaskQueue,
	}, cloud.GreetingWorkflow, "Temporal")
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
