package dataplane_test

import (
	"context"
	"strings"
	"testing"

	prometheustest "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
	"go.temporal.io/api/workflowservice/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/temporalio/temporal-proxy/internal/dataplane/dataplanetest"
	"github.com/temporalio/temporal-proxy/internal/transport/meta"
)

const getSystemInfo = "/temporal.api.workflowservice.v1.WorkflowService/GetSystemInfo"

// panickingAuth panics authenticating GetSystemInfo and admits everything else,
// so the gateway's own health streams pass through it untouched.
type panickingAuth struct{}

func (panickingAuth) Authenticate(_ context.Context, target meta.Target, _ metadata.MD) error {
	if target.FullName == getSystemInfo {
		panic("secret request contents")
	}

	return nil
}

func (panickingAuth) SecureHeaders() []string { return nil }

// TestGatewayRecoversPanicInItsInterceptorChain drives a panic through a wired
// dataplane: authentication sits late in the gateway's chain, so recovering it
// proves the recovery interceptor is installed, and the request being recorded
// as Internal proves it sits after the reporter.
func TestGatewayRecoversPanicInItsInterceptorChain(t *testing.T) {
	t.Parallel()

	up := dataplanetest.NewUpstream(t)
	f := dataplanetest.Start(t, dataplanetest.Config(up), dataplanetest.WithAuth(panickingAuth{}))

	_, err := f.Client().GetSystemInfo(
		f.Context(), &workflowservice.GetSystemInfoRequest{}, grpc.WaitForReady(true),
	)

	st, ok := status.FromError(err)
	require.True(t, ok, "want a status error, got %v", err)
	require.Equal(t, codes.Internal, st.Code())
	require.Equal(t, "internal error", st.Message())
	require.Nil(t, up.Metadata(), "the request must not have reached the upstream")

	// dataplanetest registers every collector under the "test" prefix.
	require.NoError(t, prometheustest.GatherAndCompare(f.Gatherer(), strings.NewReader(`
# HELP test_server_panics_total Total panics recovered while serving an RPC, labeled by method.
# TYPE test_server_panics_total counter
test_server_panics_total{method="`+getSystemInfo+`"} 1
`), "test_server_panics_total"))

	require.InDelta(t, 1, requestsTotal(t, f, getSystemInfo, codes.Internal), 0)
}

// requestsTotal returns requests_total for one method and code. Read on its own
// rather than compared whole, since the gateway's health streams add series of
// their own.
func requestsTotal(t *testing.T, f *dataplanetest.Fixture, method string, code codes.Code) float64 {
	t.Helper()

	mfs, err := f.Gatherer().Gather()
	require.NoError(t, err)

	for _, mf := range mfs {
		if mf.GetName() != "test_server_requests_total" {
			continue
		}

		for _, m := range mf.GetMetric() {
			labels := map[string]string{}
			for _, l := range m.GetLabel() {
				labels[l.GetName()] = l.GetValue()
			}

			if labels["method"] == method && labels["code"] == code.String() {
				return m.GetCounter().GetValue()
			}
		}
	}

	t.Fatalf("no requests_total series for %s %s", method, code)

	return 0
}
