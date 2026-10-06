package dataplane_test

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/api/common/v1"
	"go.temporal.io/api/query/v1"
	"go.temporal.io/api/workflowservice/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/temporalio/temporal-proxy/internal/config"
	"github.com/temporalio/temporal-proxy/internal/dataplane/dataplanetest"
)

// gRPC's own default would refuse anything over 4MiB from the upstream, so the
// default case only passes if the proxy raised it; the configured case only
// passes if the proxy honours the upstream's connection block.
func TestGatewayAppliesTheUpstreamResponseLimit(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		limit    config.ByteSize
		size     int
		wantCode codes.Code
	}{
		{name: "the default accepts more than gRPC's 4MiB", size: 5 << 20, wantCode: codes.OK},
		{name: "a configured limit is enforced", limit: 1 << 20, size: 2 << 20, wantCode: codes.ResourceExhausted},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			up := dataplanetest.NewUpstream(t)
			result := &common.Payloads{Payloads: []*common.Payload{{Data: bytes.Repeat([]byte("x"), tt.size)}}}
			up.SetQueryResult(result)

			cfg := dataplanetest.Config(up)
			cfg.Upstreams[0].Connection.MaxResponseSize = tt.limit
			f := dataplanetest.Start(t, cfg)

			req := &workflowservice.QueryWorkflowRequest{
				Namespace: "ns1",
				Execution: &common.WorkflowExecution{WorkflowId: "wf-1"},
				Query:     &query.WorkflowQuery{QueryType: "state"},
			}

			// The worker side allows far more than the response, so a refusal can
			// only come from the proxy's upstream connection.
			resp, err := f.Client().QueryWorkflow(
				f.Context(),
				req,
				grpc.WaitForReady(true),
				grpc.MaxCallRecvMsgSize(64<<20),
			)
			require.Equal(t, tt.wantCode, status.Code(err), "err: %v", err)
			if tt.wantCode == codes.OK {
				require.Len(t, resp.GetQueryResult().GetPayloads()[0].GetData(), tt.size)
			}
		})
	}
}

// Each upstream connection reaches the fake from its own source port, so the
// distinct peers it saw are the connections the proxy spread calls across. A
// proxy that ignored maxConnections would show one peer in both cases.
func TestUpstreamCallsSpreadAcrossMaxConnections(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		max       int
		wantPeers int
	}{
		{name: "the default pool uses 32 connections", wantPeers: 32},
		{name: "a configured pool uses every connection", max: 3, wantPeers: 3},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			up := dataplanetest.NewUpstream(t)
			cfg := dataplanetest.Config(up)
			cfg.Upstreams[0].Connection.MaxConnections = tt.max
			f := dataplanetest.Start(t, cfg)

			for range 64 {
				_, err := f.Client().GetSystemInfo(
					f.Context(),
					&workflowservice.GetSystemInfoRequest{},
					grpc.WaitForReady(true),
				)
				require.NoError(t, err)
			}

			require.Len(t, up.Peers(), tt.wantPeers)
		})
	}
}
