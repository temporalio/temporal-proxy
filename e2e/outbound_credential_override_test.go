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

// TestEndToEndOutboundCredentialOverridesForwardedHeader proves the outbound
// strip holds even with no inbound auth configured: a caller-supplied
// authorization header has nothing to authenticate against at the inbound
// server (so the call succeeds regardless), but the outbound static credential
// provider must still strip that forwarded header and replace it with the
// configured API key rather than let both reach the upstream.
func TestEndToEndOutboundCredentialOverridesForwardedHeader(t *testing.T) {
	t.Parallel()

	up := dataplanetest.NewTLSUpstream(t)

	cfg := dataplanetest.Config(up)
	cfg.Upstreams[0].Credentials = &config.CredentialConfig{
		Static: &config.StaticCredentialConfig{APIKey: "k3y"},
	}

	f := dataplanetest.StartApp(t, cfg)

	ctx := metadata.AppendToOutgoingContext(f.Context(), "authorization", "Bearer stray")
	_, err := f.Client().GetSystemInfo(ctx, &workflowservice.GetSystemInfoRequest{}, grpc.WaitForReady(true))
	require.NoError(t, err, "with no inbound auth configured, the stray header alone must not block the call")

	require.Equal(t, []string{"Bearer k3y"}, up.Metadata().Get("authorization"),
		"the outbound credential must override the forwarded header, not add to it")
}
