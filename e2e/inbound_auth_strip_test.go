package e2e

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/api/workflowservice/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/temporalio/temporal-proxy/internal/config"
	"github.com/temporalio/temporal-proxy/internal/dataplane/dataplanetest"
)

// TestEndToEndInboundAuthStrippedOutboundCredentialAttached drives a request
// through the full stack (client -> inbound server with StaticToken auth ->
// router -> per-upstream proxy -> fake TLS upstream) and proves the inbound and
// outbound header strips compose: the caller's worker token authenticates at
// the inbound server but never reaches the upstream, and the upstream sees
// exactly one authorization value, the configured static API key.
//
// TestProxyAttachesUpstreamCredential (in upstream_credential_socket_test.go)
// dials the per-upstream proxy's socket directly and so cannot exercise the
// inbound server or router at all; this test is the only one that proves both
// strips hold together end to end.
func TestEndToEndInboundAuthStrippedOutboundCredentialAttached(t *testing.T) {
	t.Parallel()

	up := dataplanetest.NewTLSUpstream(t)

	cfg := dataplanetest.Config(up)
	cfg.Auth = &config.AuthConfig{StaticToken: &config.StaticTokenConfig{Token: "worker-secret"}}
	cfg.Upstreams[0].Credentials = &config.CredentialConfig{
		Static: &config.StaticCredentialConfig{APIKey: "k3y"},
	}

	f := dataplanetest.StartApp(t, cfg)

	// The client presents the worker's own token. The inbound authenticator
	// consumes it (proving inbound auth is wired end to end); the router then
	// forwards to the upstream, whose static credential provider replaces
	// whatever authorization header survives forwarding.
	ctx := metadata.AppendToOutgoingContext(f.Context(), "authorization", "Bearer worker-secret")
	_, err := f.Client().GetSystemInfo(ctx, &workflowservice.GetSystemInfoRequest{}, grpc.WaitForReady(true))
	require.NoError(t, err, "inbound auth must accept the correct worker token")

	require.Equal(t, []string{"Bearer k3y"}, up.Metadata().Get("authorization"),
		"upstream must see only the API key; the worker token must be stripped, not forwarded or duplicated")

	// Negative check: a wrong worker token never gets past the inbound
	// authenticator, so it never reaches the router or upstream at all.
	badCtx := metadata.AppendToOutgoingContext(f.Context(), "authorization", "Bearer wrong")
	_, err = f.Client().GetSystemInfo(badCtx, &workflowservice.GetSystemInfoRequest{})
	require.Equal(t, codes.Unauthenticated, status.Code(err))
}
