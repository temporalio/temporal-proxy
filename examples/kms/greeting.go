// Package kms contains a minimal workflow used by the extension server KMS
// example: a worker and a starter share the workflow, activity, and task queue
// defined here.
package kms

import (
	"context"
	"time"

	"go.temporal.io/sdk/workflow"
)

const (
	// TaskQueue is the task queue the worker listens on and the starter targets.
	TaskQueue = "kms-example"

	// Namespace is the namespace the dev server creates by default. It is also
	// what the provider derives its wrapping key from.
	Namespace = "default"

	// ProxyHostPort is the proxy's gateway. The dev server holds 7233, so the
	// gateway listens next to it on 7234.
	ProxyHostPort = "127.0.0.1:7234"
)

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
