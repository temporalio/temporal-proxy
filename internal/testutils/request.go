// Package testutils holds helpers shared between the internal packages' test
// suites.
package testutils

import (
	"go.temporal.io/api/common/v1"
	"go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
)

// StartWorkflowRequest builds a StartWorkflowExecutionRequest shaped like the
// ones SDK clients send: identity fields, a typed workflow and task queue, and
// a 1 KiB input payload, so extraction and translation measure a realistic
// unmarshal rather than a two-field toy message.
func StartWorkflowRequest() *workflowservice.StartWorkflowExecutionRequest {
	input := make([]byte, 1024)
	for i := range input {
		input[i] = byte('a' + i%26)
	}

	return &workflowservice.StartWorkflowExecutionRequest{
		Namespace:    "payments-production",
		WorkflowId:   "billing-run-2026-10-10-abc123",
		WorkflowType: &common.WorkflowType{Name: "billing.daily-run"},
		TaskQueue:    &taskqueue.TaskQueue{Name: "billing-tq"},
		Input: &common.Payloads{Payloads: []*common.Payload{{
			Metadata: map[string][]byte{"encoding": []byte("json/plain")},
			Data:     input,
		}}},
		Identity: "1@worker-7",
	}
}
