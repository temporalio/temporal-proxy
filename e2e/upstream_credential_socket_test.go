package e2e

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/api/workflowservice/v1"
	"google.golang.org/grpc"

	"github.com/temporalio/temporal-proxy/internal/config"
	"github.com/temporalio/temporal-proxy/internal/dataplane/dataplanetest"
)

// TestProxyAttachesUpstreamCredential proves the static credential configured
// on an upstream actually reaches that upstream: it stands up a fake
// WorkflowService server over TLS, points a real dataplane at it
// with a static credential configured, drives a request through the proxy's
// local socket, and asserts the fake upstream observed the
// "authorization: Bearer <key>" header. The static provider requires
// transport security, so this exercises the same TLS + per-RPC credential
// dial path production traffic takes; construction in isolation cannot prove
// the header actually arrives on the wire.
func TestProxyAttachesUpstreamCredential(t *testing.T) {
	t.Parallel()

	up := dataplanetest.NewTLSUpstream(t)

	cfg := dataplanetest.Config(up)
	cfg.Upstreams[0].Credentials = &config.CredentialConfig{
		Static: &config.StaticCredentialConfig{APIKey: "k3y"},
	}

	f := dataplanetest.Start(t, cfg)

	conn := f.UpstreamConn(dataplanetest.DefaultUpstream)
	_, err := workflowservice.NewWorkflowServiceClient(conn).GetSystemInfo(
		f.Context(), &workflowservice.GetSystemInfoRequest{}, grpc.WaitForReady(true),
	)
	require.NoError(t, err)

	require.Equal(t, []string{"Bearer k3y"}, up.Metadata().Get("authorization"),
		"expected the configured static credential to reach the upstream")
}
