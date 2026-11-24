// Command starter calls HelloOperation through the proxy at localhost:7234
// twice: from CallerWorkflow, and as a standalone Nexus operation. It prints
// both results. The name to greet is the first argument, "Temporal" by default.
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"go.temporal.io/sdk/client"

	hello "go.temporal.io/proxy/examples/nexus"
)

func main() {
	name := "Temporal"
	if len(os.Args) > 1 {
		name = os.Args[1]
	}

	c, err := client.Dial(client.Options{HostPort: "localhost:7234"})
	if err != nil {
		log.Fatalf("dial proxy: %v", err)
	}
	defer c.Close()

	ctx := context.Background()

	fromWorkflow, err := callFromWorkflow(ctx, c, name)
	if err != nil {
		log.Fatalf("workflow caller: %v", err)
	}
	fmt.Println("from workflow:  ", fromWorkflow)

	standalone, err := callStandalone(ctx, c, name)
	if err != nil {
		log.Fatalf("standalone caller: %v", err)
	}
	fmt.Println("standalone:     ", standalone)
}

// callFromWorkflow runs CallerWorkflow, which schedules HelloOperation.
func callFromWorkflow(ctx context.Context, c client.Client, name string) (string, error) {
	run, err := c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		ID:        "nexus-example-caller",
		TaskQueue: hello.TaskQueue,
	}, hello.CallerWorkflow, name)
	if err != nil {
		return "", fmt.Errorf("start workflow: %w", err)
	}
	log.Printf("started workflow id=%s runID=%s", run.GetID(), run.GetRunID())

	var greeting string
	if err := run.Get(ctx, &greeting); err != nil {
		return "", fmt.Errorf("get result: %w", err)
	}

	return greeting, nil
}

// callStandalone starts HelloOperation directly from the client, with no
// workflow in between.
func callStandalone(ctx context.Context, c client.Client, name string) (string, error) {
	nc, err := c.NewNexusClient(client.NexusClientOptions{
		Endpoint: hello.Endpoint,
		Service:  hello.Service,
	})
	if err != nil {
		return "", fmt.Errorf("nexus client: %w", err)
	}

	h, err := nc.ExecuteOperation(ctx, hello.HelloOperation, name, client.StartNexusOperationOptions{
		ID:                     "nexus-example-standalone",
		ScheduleToCloseTimeout: time.Minute,
	})
	if err != nil {
		return "", fmt.Errorf("start operation: %w", err)
	}
	log.Printf("started operation id=%s runID=%s", h.GetID(), h.GetRunID())

	var greeting string
	if err := h.Get(ctx, &greeting); err != nil {
		return "", fmt.Errorf("get result: %w", err)
	}

	return greeting, nil
}
