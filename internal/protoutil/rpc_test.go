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

func TestSplitMethod(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		in      string
		service string
		method  string
		ok      bool
	}{
		{
			name:    "full method as gRPC supplies it",
			in:      "/temporal.api.workflowservice.v1.WorkflowService/StartWorkflowExecution",
			service: "temporal.api.workflowservice.v1.WorkflowService",
			method:  "StartWorkflowExecution",
			ok:      true,
		},
		{
			name:    "full method without a leading slash",
			in:      "temporal.api.workflowservice.v1.WorkflowService/StartWorkflowExecution",
			service: "temporal.api.workflowservice.v1.WorkflowService",
			method:  "StartWorkflowExecution",
			ok:      true,
		},
		{
			// The split succeeds and the caller decides an empty method is not
			// resolvable, rather than this reporting a malformed name.
			name:    "an empty method splits with an empty half",
			in:      "/pkg.Service/",
			service: "pkg.Service",
			method:  "",
			ok:      true,
		},
		{
			name: "a bare service name has no method to split off",
			in:   "pkg.Service",
			ok:   false,
		},
		{
			name: "the empty string does not split",
			in:   "",
			ok:   false,
		},
		{
			// The split is on the last slash, so a method name is never mistaken
			// for part of the service.
			name:    "only the final segment is the method",
			in:      "/a/b/c",
			service: "a/b",
			method:  "c",
			ok:      true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			service, method, ok := protoutil.SplitMethod(tt.in)
			require.Equal(t, tt.ok, ok)
			require.Equal(t, tt.service, service)
			require.Equal(t, tt.method, method)
		})
	}
}
