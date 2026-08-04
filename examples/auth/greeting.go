// Package auth contains the pieces the inbound authentication example's worker
// and starter share: the workflow they run, and the credentials they present to
// the proxy's gateway.
package auth

import (
	"context"
	"time"

	"go.temporal.io/sdk/workflow"
)

const (
	// TaskQueue is the task queue the worker listens on and the starter targets.
	TaskQueue = "auth-example"

	// Namespace is the namespace the dev server creates by default.
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
