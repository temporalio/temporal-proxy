package e2e

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/api/workflowservice/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/temporalio/temporal-proxy/internal/config"
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

	up := newFakeTLSUpstream(t)

	inboundAddr := freeTCPAddr(t)
	app := newFullApp(t, &config.Config{
		Listen:  config.ListenConfig{HostPort: inboundAddr},
		Auth:    &config.AuthConfig{StaticToken: &config.StaticTokenConfig{Token: "worker-secret"}},
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

	// The client presents the worker's own token. The inbound authenticator
	// consumes it (proving inbound auth is wired end to end); the router then
	// forwards to the "workers" upstream, whose static credential provider
	// replaces whatever authorization header survives forwarding.
	ctx := metadata.AppendToOutgoingContext(startCtx, "authorization", "Bearer worker-secret")
	_, err := workflowservice.NewWorkflowServiceClient(conn).GetSystemInfo(
		ctx, &workflowservice.GetSystemInfoRequest{}, grpc.WaitForReady(true),
	)
	require.NoError(t, err, "inbound auth must accept the correct worker token")

	got := up.svc.received().Get("authorization")
	require.Equal(t, []string{"Bearer k3y"}, got,
		"upstream must see only the API key; the worker token must be stripped, not forwarded or duplicated")

	// Negative check: a wrong worker token never gets past the inbound
	// authenticator, so it never reaches the router or upstream at all.
	badCtx := metadata.AppendToOutgoingContext(startCtx, "authorization", "Bearer wrong")
	_, err = workflowservice.NewWorkflowServiceClient(conn).GetSystemInfo(badCtx, &workflowservice.GetSystemInfoRequest{})
	require.Equal(t, codes.Unauthenticated, status.Code(err))
}
