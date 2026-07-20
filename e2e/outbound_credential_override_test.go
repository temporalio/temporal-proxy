package e2e

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/api/workflowservice/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	"github.com/temporalio/temporal-proxy/internal/config"
)

// TestEndToEndOutboundCredentialOverridesForwardedHeader proves the outbound
// strip holds even with no inbound auth configured: a caller-supplied
// authorization header has nothing to authenticate against at the inbound
// server (so the call succeeds regardless), but the outbound static credential
// provider must still strip that forwarded header and replace it with the
// configured API key rather than let both reach the upstream.
func TestEndToEndOutboundCredentialOverridesForwardedHeader(t *testing.T) {
	t.Parallel()

	up := newFakeTLSUpstream(t)

	inboundAddr := freeTCPAddr(t)
	app := newFullApp(t, &config.Config{
		Listen:  config.ListenConfig{HostPort: inboundAddr},
		Routing: config.Routing{DefaultUpstream: "workers"},
		Upstreams: []config.Upstream{{
			Name: "workers",
			Listen: config.ListenConfig{
				HostPort: up.addr,
				TLS: &config.TLSConfig{
					CA:         up.caFile,
					Cert:       up.clientCertFile,
					Key:        up.clientKeyFile,
					ServerName: "localhost",
				},
			},
			Credentials: &config.CredentialConfig{Static: &config.StaticCredentialConfig{APIKey: "k3y"}},
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

	ctx := metadata.AppendToOutgoingContext(startCtx, "authorization", "Bearer stray")
	_, err := workflowservice.NewWorkflowServiceClient(conn).GetSystemInfo(
		ctx, &workflowservice.GetSystemInfoRequest{}, grpc.WaitForReady(true),
	)
	require.NoError(t, err, "with no inbound auth configured, the stray header alone must not block the call")

	require.Equal(t, []string{"Bearer k3y"}, up.svc.received().Get("authorization"),
		"the outbound credential must override the forwarded header, not add to it")
}
