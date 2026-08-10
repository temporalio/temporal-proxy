package dataplanetest_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/api/common/v1"
	"go.temporal.io/api/query/v1"
	"go.temporal.io/api/workflowservice/v1"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"

	"github.com/temporalio/temporal-proxy/internal/dataplane/dataplanetest"
)

// TestTLSUpstreamIsReachableWithItsOwnTLSConfig proves the config the fake
// hands back actually dials it, which is what lets a test exercise credentials
// that refuse an insecure transport.
func TestTLSUpstreamIsReachableWithItsOwnTLSConfig(t *testing.T) {
	t.Parallel()

	up := dataplanetest.NewTLSUpstream(t)
	require.NotNil(t, up.TLSConfig())

	f := dataplanetest.Start(t, dataplanetest.Config(up))

	_, err := f.Client().GetSystemInfo(
		f.Context(), &workflowservice.GetSystemInfoRequest{}, grpc.WaitForReady(true),
	)
	require.NoError(t, err)
	require.NotNil(t, up.Metadata())
}

// TestUpstreamRecordsAndEchoesQueryWorkflow covers the pair of behaviours a
// payload-rewriting interceptor needs: the request body is recorded as the
// upstream saw it, and the arguments come back as the result so one call
// exercises both directions.
func TestUpstreamRecordsAndEchoesQueryWorkflow(t *testing.T) {
	t.Parallel()

	up := dataplanetest.NewUpstream(t)
	f := dataplanetest.Start(t, dataplanetest.Config(up))

	args := &common.Payloads{Payloads: []*common.Payload{{Data: []byte(`"hello"`)}}}
	resp, err := f.Client().QueryWorkflow(
		f.Context(),
		&workflowservice.QueryWorkflowRequest{
			Namespace: "ns1",
			Execution: &common.WorkflowExecution{WorkflowId: "wf-1"},
			Query:     &query.WorkflowQuery{QueryType: "state", QueryArgs: args},
		},
		grpc.WaitForReady(true),
	)
	require.NoError(t, err)
	require.True(t, proto.Equal(args, resp.GetQueryResult()), "the fake echoes the query arguments back")

	reqs := up.Requests()
	require.Len(t, reqs, 1)

	got, ok := reqs[0].(*workflowservice.QueryWorkflowRequest)
	require.True(t, ok, "the recorded request keeps its concrete type")
	require.True(t, proto.Equal(args, got.GetQuery().GetQueryArgs()))
}

func TestUpstreamPlaintextHasNoTLSConfig(t *testing.T) {
	t.Parallel()

	require.Nil(t, dataplanetest.NewUpstream(t).TLSConfig())
}
