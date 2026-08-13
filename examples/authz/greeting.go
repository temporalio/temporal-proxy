// Package authz contains what the authorization example's commands share: a
// minimal workflow for the worker and starter, and the token contract the
// gentoken command writes and the extension server reads.
package authz

import (
	"context"
	"time"

	"go.temporal.io/sdk/workflow"
)

// TaskQueue is the task queue the worker listens on and the starter targets.
const TaskQueue = "authz-example"

// GreetingWorkflow runs ComposeGreeting and returns its result.
func GreetingWorkflow(ctx workflow.Context, name string) (string, error) {
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: time.Minute,
	})

	var greeting string
	if err := workflow.ExecuteActivity(ctx, ComposeGreeting, name).Get(ctx, &greeting); err != nil {
		return "", err
	}

	return greeting, nil
}

// ComposeGreeting returns a greeting for name.
func ComposeGreeting(_ context.Context, name string) (string, error) {
	return "Hello, " + name + "!", nil
}
