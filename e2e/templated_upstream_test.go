package e2e

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/api/workflowservice/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	"github.com/temporalio/temporal-proxy/internal/config"
)

// TestEndToEndTemplatedUpstreamRoutesByRenderedAddress drives the full stack
// with a single templated upstream whose hostPort renders from request
// metadata, and proves each request reaches the upstream its metadata names.
// This exercises per-request address rendering end to end, and because two
// distinct rendered targets are both reached, it also proves the connection
// pool keys by rendered target rather than collapsing onto the first dial.
func TestEndToEndTemplatedUpstreamRoutesByRenderedAddress(t *testing.T) {
	t.Parallel()

	svcA, addrA := newFakeUpstream(t)
	svcB, addrB := newFakeUpstream(t)

	inboundAddr := freeTCPAddr(t)
	app := newFullApp(t, &config.Config{
		Listen:  config.ListenConfig{HostPort: inboundAddr},
		Routing: config.Routing{DefaultUpstream: "dynamic"},
		Upstreams: []config.Upstream{{
			Name:   "dynamic",
			Listen: config.ListenConfig{HostPort: `{{ index .Metadata "x-upstream" }}`},
		}},
	})
	require.NoError(t, app.Err())

	startCtx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	require.NoError(t, app.Start(startCtx))
	t.Cleanup(func() {
		stopCtx, stopCancel := context.WithTimeout(t.Context(), 5*time.Second)
		defer stopCancel()
		_ = app.Stop(stopCtx)
	})

	conn := dialInbound(t, inboundAddr)
	defer func() { _ = conn.Close() }()
	client := workflowservice.NewWorkflowServiceClient(conn)

	call := func(target string) {
		ctx := metadata.AppendToOutgoingContext(startCtx, "x-upstream", target)
		_, err := client.GetSystemInfo(ctx, &workflowservice.GetSystemInfoRequest{}, grpc.WaitForReady(true))
		require.NoError(t, err)
	}

	call(addrA)
	call(addrB)

	require.NotNil(t, svcA.received(), "the request naming upstream A must reach A")
	require.NotNil(t, svcB.received(), "the request naming upstream B must reach B")
	require.Equal(t, []string{addrA}, svcA.received().Get("x-upstream"))
	require.Equal(t, []string{addrB}, svcB.received().Get("x-upstream"))
}

// newFakeUpstream stands up a plaintext fake WorkflowService frontend and
// returns it with its dial address.
func newFakeUpstream(t *testing.T) (*capturingWorkflowService, string) {
	t.Helper()

	svc := &capturingWorkflowService{}
	srv := grpc.NewServer()
	workflowservice.RegisterWorkflowServiceServer(srv, svc)

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)

	return svc, lis.Addr().String()
}
