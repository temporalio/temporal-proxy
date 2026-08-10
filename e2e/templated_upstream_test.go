package e2e

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/api/workflowservice/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	"github.com/temporalio/temporal-proxy/internal/config"
	"github.com/temporalio/temporal-proxy/internal/dataplane/dataplanetest"
)

// TestEndToEndTemplatedUpstreamRoutesByRenderedAddress drives the full stack
// with a single templated upstream whose hostPort renders from request
// metadata, and proves each request reaches the upstream its metadata names.
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
			Listen: config.ListenConfig{HostPort: `{{ index .Metadata "x-upstream" }}`},
		}},
	})

	call := func(target string) {
		ctx := metadata.AppendToOutgoingContext(f.Context(), "x-upstream", target)
		_, err := f.Client().GetSystemInfo(ctx, &workflowservice.GetSystemInfoRequest{}, grpc.WaitForReady(true))
		require.NoError(t, err)
	}

	call(upA.Addr())
	call(upB.Addr())

	require.NotNil(t, upA.Metadata(), "the request naming upstream A must reach A")
	require.NotNil(t, upB.Metadata(), "the request naming upstream B must reach B")
	require.Equal(t, []string{upA.Addr()}, upA.Metadata().Get("x-upstream"))
	require.Equal(t, []string{upB.Addr()}, upB.Metadata().Get("x-upstream"))
}
