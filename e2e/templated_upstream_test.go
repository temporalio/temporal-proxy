package e2e

import (
	"net"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/api/workflowservice/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	"go.temporal.io/proxy/internal/config"
	"go.temporal.io/proxy/internal/dataplane/dataplanetest"
)

// TestEndToEndTemplatedUpstreamRoutesByRenderedAddress drives the full stack
// with a single templated upstream whose port renders from request metadata,
// and proves each request reaches the upstream its metadata names.
// This exercises per-request address rendering end to end, and because two
// distinct rendered targets are both reached, it also proves the connection
// pool keys by rendered target rather than collapsing onto the first dial.
func TestEndToEndTemplatedUpstreamRoutesByRenderedAddress(t *testing.T) {
	t.Parallel()

	upA := dataplanetest.NewUpstream(t)
	upB := dataplanetest.NewUpstream(t)

	f := dataplanetest.StartApp(t, &config.Config{
		Routing: config.Routing{DefaultUpstream: "dynamic"},
		Upstreams: config.UpstreamList{{
			Name:   "dynamic",
			Listen: config.ListenConfig{HostPort: `127.0.0.1:{{ index .Metadata "x-upstream-port" }}`, Insecure: true},
		}},
	})

	portOf := func(up *dataplanetest.Upstream) string {
		_, port, err := net.SplitHostPort(up.Addr())
		require.NoError(t, err)

		return port
	}

	call := func(port string) {
		ctx := metadata.AppendToOutgoingContext(f.Context(), "x-upstream-port", port)
		_, err := f.Client().GetSystemInfo(ctx, &workflowservice.GetSystemInfoRequest{}, grpc.WaitForReady(true))
		require.NoError(t, err)
	}

	call(portOf(upA))
	call(portOf(upB))

	require.NotNil(t, upA.Metadata(), "the request naming upstream A must reach A")
	require.NotNil(t, upB.Metadata(), "the request naming upstream B must reach B")
	require.Equal(t, []string{portOf(upA)}, upA.Metadata().Get("x-upstream-port"))
	require.Equal(t, []string{portOf(upB)}, upB.Metadata().Get("x-upstream-port"))
}
