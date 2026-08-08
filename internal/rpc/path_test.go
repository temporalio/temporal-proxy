package rpc_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/temporalio/temporal-proxy/internal/rpc"
	"github.com/temporalio/temporal-proxy/pkg/testutil"
)

func TestFullMethod(t *testing.T) {
	t.Parallel()

	const method = "/temporal.api.workflowservice.v1.WorkflowService/StartWorkflowExecution"

	ctx := grpc.NewContextWithServerTransportStream(
		t.Context(),
		testutil.ServerTransportStream{FullMethodName: method},
	)

	got, err := rpc.FullMethod(ctx)
	require.NoError(t, err)
	require.Equal(t, method, got)

	// A context that is not a server call's carries no method, and saying so as
	// Internal keeps a handler from forwarding a call it cannot name.
	got, err = rpc.FullMethod(t.Context())
	require.Equal(t, codes.Internal, status.Code(err))
	require.ErrorContains(t, err, "no server transport stream")
	require.Empty(t, got)
}

func TestServiceMethod(t *testing.T) {
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
			name: "a lone slash does not split",
			in:   "/",
			ok:   false,
		},
		{
			// Only the leading slash is trimmed, so a doubled one leaves an empty
			// first segment rather than being silently normalised.
			name:    "a doubled leading slash keeps the empty segment",
			in:      "//pkg.Service/Method",
			service: "/pkg.Service",
			method:  "Method",
			ok:      true,
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

			service, method, ok := rpc.ServiceMethod(tt.in)
			require.Equal(t, tt.ok, ok)
			require.Equal(t, tt.service, service)
			require.Equal(t, tt.method, method)

			// The accessors drop the ok, so a name that does not split must reach
			// their callers as "" rather than as a name to match on.
			require.Equal(t, tt.service, rpc.Service(tt.in))
			require.Equal(t, tt.method, rpc.Method(tt.in))
		})
	}
}
