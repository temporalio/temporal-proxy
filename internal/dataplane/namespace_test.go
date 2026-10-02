package dataplane_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/api/workflowservice/v1"
	"google.golang.org/grpc/metadata"

	"github.com/temporalio/temporal-proxy/internal/dataplane/dataplanetest"
	"github.com/temporalio/temporal-proxy/internal/transport/meta"
)

func TestGatewayOverridesAClientSuppliedNamespaceHeader(t *testing.T) {
	t.Parallel()

	// Guard: the namespace header is trusted downstream of the gateway. Templated
	// upstreams resolve their host from it and payload encryption picks its key
	// by it, so a client sending its own must not get it past the gateway, which
	// stamps the namespace it read from the request instead.
	up := dataplanetest.NewUpstream(t)
	f := dataplanetest.Start(t, dataplanetest.Config(up))

	ctx := metadata.AppendToOutgoingContext(f.Context(), meta.NamespaceHeader, "forged")
	_, err := f.Client().QueryWorkflow(ctx, &workflowservice.QueryWorkflowRequest{Namespace: "orders"})
	require.NoError(t, err)

	require.Equal(t, []string{"orders"}, up.Metadata().Get(meta.NamespaceHeader))
}
