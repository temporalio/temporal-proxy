package dataplane_test

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/api/workflowservice/v1"

	"github.com/temporalio/temporal-proxy/internal/dataplane/dataplanetest"
)

func TestGatewayForwardsWithoutTheUpstreamSocket(t *testing.T) {
	t.Parallel()

	// Guard: the gateway serves each upstream in process rather than over the
	// upstream's unix socket. Both ends of that connection would be in this
	// process, and a burst of cancellations under load can leave gRPC's reader
	// and writer blocked on each other at both ends at once, hanging every
	// request the gateway forwards. With the socket file gone, a gateway that
	// still dialled it could not forward at all.
	up := dataplanetest.NewUpstream(t)
	f := dataplanetest.Start(t, dataplanetest.Config(up))

	require.NoError(t, os.Remove(f.SocketPath(dataplanetest.DefaultUpstream)))

	_, err := f.Client().GetSystemInfo(f.Context(), &workflowservice.GetSystemInfoRequest{})
	require.NoError(t, err)
}
