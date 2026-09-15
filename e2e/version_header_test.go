package e2e

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/api/workflowservice/v1"
	cloudnamespace "go.temporal.io/cloud-sdk/api/namespace/v1"
	"google.golang.org/grpc"

	"github.com/temporalio/temporal-proxy/internal/config"
	"github.com/temporalio/temporal-proxy/internal/dataplane/dataplanetest"
	"github.com/temporalio/temporal-proxy/internal/transport/meta"
	"github.com/temporalio/temporal-proxy/internal/version"
)

// TestEndToEndTheCloudAPIConnectionCarriesTheProxyVersion covers the second hop
// a request can take onto Temporal Cloud. A translated method leaves the
// frontend connection and travels a separate connection to the control plane,
// which is dialled by its own code path, so the version header has to be
// installed there as well as on the frontend.
func TestEndToEndTheCloudAPIConnectionCarriesTheProxyVersion(t *testing.T) {
	t.Parallel()

	frontend := dataplanetest.NewUpstream(t)
	cloud := newCloudUpstream(t)
	cloud.setPage(&cloudnamespace.Namespace{Namespace: "payments.a1b2c"})

	f := dataplanetest.StartApp(t, &config.Config{
		Routing: config.Routing{DefaultUpstream: "frontend", SystemUpstream: "frontend"},
		Upstreams: config.UpstreamList{{
			Name:   "frontend",
			Cloud:  true,
			Listen: frontend.Listen(),
		}},
		APITranslations: cloudAPIAt(cloud.addr),
	})

	_, err := f.Client().ListNamespaces(
		f.Context(),
		&workflowservice.ListNamespacesRequest{},
		grpc.WaitForReady(true),
	)
	require.NoError(t, err)

	require.NotNil(t, cloud.request(), "the Cloud API must have been called")
	require.Equal(t, []string{version.Version}, cloud.metadata().Get(meta.VersionHeader))
}

// TestEndToEndOnlyTheCloudUpstreamIsSentTheProxyVersion is the hybrid
// deployment: Temporal Cloud serves the namespaces routed to it by rule while a
// self-hosted Temporal Service serves the rest, both behind one proxy. The
// header follows the upstream rather than the process, so the same running proxy
// sends its version on one connection and nothing on the other. A Temporal
// Service has no use for the proxy's build identity and is not told it.
func TestEndToEndOnlyTheCloudUpstreamIsSentTheProxyVersion(t *testing.T) {
	t.Parallel()

	onprem := dataplanetest.NewUpstream(t)
	cloud := dataplanetest.NewUpstream(t)

	f := dataplanetest.StartApp(t, &config.Config{
		Routing: config.Routing{
			DefaultUpstream: "onprem",
			SystemUpstream:  "onprem",
			Rules: []config.RoutingRule{{
				Upstream: "cloud",
				Match:    config.RoutingMatch{Namespace: "*.a1b2c"},
			}},
		},
		Upstreams: config.UpstreamList{
			{Name: "onprem", Listen: onprem.Listen()},
			{Name: "cloud", Cloud: true, Listen: cloud.Listen()},
		},
	})

	// A namespace matching the rule is served by Cloud.
	_, err := f.Client().QueryWorkflow(
		f.Context(),
		&workflowservice.QueryWorkflowRequest{Namespace: "payments.a1b2c"},
		grpc.WaitForReady(true),
	)
	require.NoError(t, err)

	// Every other namespace is served by the Temporal Service.
	_, err = f.Client().QueryWorkflow(
		f.Context(),
		&workflowservice.QueryWorkflowRequest{Namespace: "orders"},
		grpc.WaitForReady(true),
	)
	require.NoError(t, err)

	require.NotNil(t, cloud.Metadata(), "the Cloud upstream must have been called")
	require.Equal(t, []string{version.Version}, cloud.Metadata().Get(meta.VersionHeader))

	require.NotNil(t, onprem.Metadata(), "the Temporal Service must have been called")
	require.Empty(t, onprem.Metadata().Get(meta.VersionHeader),
		"a self-hosted Temporal Service is not told the proxy's version")
}
