package protoutil_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/temporalio/temporal-proxy/internal/protoutil"
)

func TestServiceName(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		in   string
		want string
	}{
		{
			name: "full method as gRPC supplies it",
			in:   "/temporal.api.workflowservice.v1.WorkflowService/StartWorkflowExecution",
			want: "temporal.api.workflowservice.v1.WorkflowService",
		},
		{
			name: "full method without a leading slash",
			in:   "temporal.api.workflowservice.v1.WorkflowService/StartWorkflowExecution",
			want: "temporal.api.workflowservice.v1.WorkflowService",
		},
		{
			name: "a bare service name has no method to strip",
			in:   "temporal.api.workflowservice.v1.WorkflowService",
			want: "",
		},
		{
			name: "an empty method still yields the service",
			in:   "/temporal.api.workflowservice.v1.WorkflowService/",
			want: "temporal.api.workflowservice.v1.WorkflowService",
		},
		{
			name: "the empty string yields no service",
			in:   "",
			want: "",
		},
		{
			name: "a lone slash yields no service",
			in:   "/",
			want: "",
		},
		{
			// Only the leading slash is trimmed, so a doubled one leaves an
			// empty first segment rather than being silently normalised.
			name: "a doubled leading slash keeps the empty segment",
			in:   "//pkg.Service/Method",
			want: "/pkg.Service",
		},
		{
			// The split is on the last slash, so a method name is never
			// mistaken for part of the service.
			name: "only the final segment is treated as the method",
			in:   "/a/b/c",
			want: "a/b",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tt.want, protoutil.ServiceName(tt.in))
		})
	}
}
