package dataplane_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/api/common/v1"
	"go.temporal.io/api/query/v1"
	"go.temporal.io/api/workflowservice/v1"
	"google.golang.org/grpc"
	_ "google.golang.org/grpc/encoding/gzip"
	"google.golang.org/protobuf/proto"

	"github.com/temporalio/temporal-proxy/internal/dataplane/dataplanetest"
)

// A health check is served locally and never reaches the forwarder. This sends
// QueryWorkflow through that path, with and without gzip.
func TestGatewayForwardsCompressedRequests(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		opts []grpc.CallOption
	}{
		{name: "identity"},
		{name: "gzip", opts: []grpc.CallOption{grpc.UseCompressor("gzip")}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			up := dataplanetest.NewUpstream(t)
			f := dataplanetest.Start(t, dataplanetest.Config(up))

			args := &common.Payloads{Payloads: []*common.Payload{{Data: []byte(`"hello"`)}}}
			req := &workflowservice.QueryWorkflowRequest{
				Namespace: "ns1",
				Execution: &common.WorkflowExecution{WorkflowId: "wf-1"},
				Query:     &query.WorkflowQuery{QueryType: "state", QueryArgs: args},
			}

			resp, err := f.Client().QueryWorkflow(
				f.Context(),
				req,
				append(tt.opts, grpc.WaitForReady(true))...,
			)
			require.NoError(t, err)
			require.True(t, proto.Equal(args, resp.GetQueryResult()), "the response must round-trip")

			reqs := up.Requests()
			require.Len(t, reqs, 1)
			require.True(t, proto.Equal(req, reqs[0]), "the upstream must see the request unchanged")
		})
	}
}
