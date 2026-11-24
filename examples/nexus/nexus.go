// Package nexus contains the Nexus service used by the Nexus proxy example: a
// worker serves it, and a starter calls it from a workflow and as a standalone
// operation.
package nexus

import (
	"context"
	"time"

	"github.com/nexus-rpc/sdk-go/nexus"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporalnexus"
	"go.temporal.io/sdk/workflow"
)

const (
	// Endpoint is the Nexus Endpoint the README creates, targeting TaskQueue.
	Endpoint = "hello-endpoint"

	// Service is the Nexus Service name HelloOperation is registered under.
	Service = "hello-service"

	// TaskQueue is the task queue the worker listens on, for workflows and Nexus
	// tasks alike.
	TaskQueue = "nexus-example"
)

// HelloOperation starts HelloHandlerWorkflow and completes with its result.
var HelloOperation = temporalnexus.NewWorkflowRunOperation(
	"say-hello",
	HelloHandlerWorkflow,
	func(_ context.Context, _ string, opts nexus.StartOperationOptions) (client.StartWorkflowOptions, error) {
		return client.StartWorkflowOptions{ID: "nexus-example-handler-" + opts.RequestID}, nil
	},
)

// NewService returns the Nexus Service the worker registers.
func NewService() (*nexus.Service, error) {
	svc := nexus.NewService(Service)
	if err := svc.Register(HelloOperation); err != nil {
		return nil, err
	}

	return svc, nil
}

// CallerWorkflow calls HelloOperation through Endpoint and returns its result.
func CallerWorkflow(ctx workflow.Context, name string) (string, error) {
	c := workflow.NewNexusClient(Endpoint, Service)
	fut := c.ExecuteOperation(ctx, HelloOperation, name, workflow.NexusOperationOptions{
		ScheduleToCloseTimeout: time.Minute,
	})

	var greeting string
	if err := fut.Get(ctx, &greeting); err != nil {
		return "", err
	}

	return greeting, nil
}

// HelloHandlerWorkflow is the workflow behind HelloOperation.
func HelloHandlerWorkflow(_ workflow.Context, name string) (string, error) {
	return "Hello, " + name + "!", nil
}
