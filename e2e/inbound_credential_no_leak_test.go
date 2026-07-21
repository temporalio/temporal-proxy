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

// TestEndToEndInboundHeaderStrippedNotLeakedUpstream isolates the inbound
// strip's independent contribution. Here the inbound authenticator consumes a
// worker credential on its own header (x-worker-auth), distinct from the
// outbound credential's authorization header, so the outbound strip (which
// only ever touches authorization) has nothing to do with x-worker-auth: if
// it reaches the upstream at all, only the inbound strip could have failed to
// remove it. TestEndToEndInboundAuthStrippedOutboundCredentialAttached
// exercises both strips on the same header, which cannot distinguish "the
// inbound strip removed it" from "the outbound strip's replacement happened
// to overwrite it".
func TestEndToEndInboundHeaderStrippedNotLeakedUpstream(t *testing.T) {
	t.Parallel()

	up := newFakeTLSUpstream(t)
	inboundAddr := freeTCPAddr(t)

	cfg := &config.Config{
		Listen: config.ListenConfig{HostPort: inboundAddr},
		// Inbound auth consumes a custom header, distinct from the outbound credential's.
		Auth:    &config.AuthConfig{StaticToken: &config.StaticTokenConfig{Token: "worker-secret", Header: "x-worker-auth"}},
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
			Credentials: &config.CredentialConfig{Static: &config.StaticCredentialConfig{APIKey: "k3y"}}, // header defaults to authorization
		}},
	}

	app := newFullApp(t, cfg)
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

	ctx := metadata.AppendToOutgoingContext(startCtx, "x-worker-auth", "Bearer worker-secret")
	_, err := workflowservice.NewWorkflowServiceClient(conn).GetSystemInfo(ctx, &workflowservice.GetSystemInfoRequest{}, grpc.WaitForReady(true))
	require.NoError(t, err)

	got := up.svc.received()
	require.Empty(t, got.Get("x-worker-auth"), "the worker credential (on its own header) must not leak upstream")
	require.Equal(t, []string{"Bearer k3y"}, got.Get("authorization"), "the upstream must see exactly the API key")
}
