package dataplane_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/api/workflowservice/v1"
	"google.golang.org/grpc"

	"github.com/temporalio/temporal-proxy/internal/dataplane/dataplanetest"
	"github.com/temporalio/temporal-proxy/internal/transport/meta"
	"github.com/temporalio/temporal-proxy/internal/version"
)

// TestCloudUpstreamReceivesTheProxyVersion pins that Temporal Cloud is told
// which proxy build a request came from. Nothing configures it: the upstream
// being Cloud is what installs the header.
func TestCloudUpstreamReceivesTheProxyVersion(t *testing.T) {
	t.Parallel()

	up := dataplanetest.NewUpstream(t)
	cfg := dataplanetest.Config(up)
	cfg.Upstreams[0].Cloud = true

	f := dataplanetest.Start(t, cfg)

	_, err := f.Client().QueryWorkflow(
		f.Context(),
		&workflowservice.QueryWorkflowRequest{Namespace: "orders"},
		grpc.WaitForReady(true),
	)
	require.NoError(t, err)

	require.Equal(t, []string{version.Version}, up.Metadata().Get(meta.VersionHeader))
}

// TestNonCloudUpstreamReceivesNoProxyVersion pins the other half: a self-hosted
// Temporal Service is not told the proxy's build identity.
func TestNonCloudUpstreamReceivesNoProxyVersion(t *testing.T) {
	t.Parallel()

	up := dataplanetest.NewUpstream(t)
	f := dataplanetest.Start(t, dataplanetest.Config(up))

	_, err := f.Client().QueryWorkflow(
		f.Context(),
		&workflowservice.QueryWorkflowRequest{Namespace: "orders"},
		grpc.WaitForReady(true),
	)
	require.NoError(t, err)

	require.NotNil(t, up.Metadata(), "the request must have reached the fake upstream")
	require.Empty(t, up.Metadata().Get(meta.VersionHeader))
}
