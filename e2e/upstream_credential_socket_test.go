package e2e

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/api/workflowservice/v1"
	"google.golang.org/grpc"

	"github.com/temporalio/temporal-proxy/internal/config"
)

// TestProxyAttachesUpstreamCredential proves the static credential configured
// on an upstream actually reaches that upstream: it stands up a fake
// WorkflowService server over TLS, points a real [proxy.Server] (built via
// [proxy.Module]) at it with a static credential configured, drives a
// request through the proxy's local socket, and asserts the fake upstream
// observed the "authorization: Bearer <key>" header. The static provider
// requires transport security, so this exercises the same TLS + per-RPC
// credential dial path production traffic takes; fx.go wiring in isolation
// cannot prove the header actually arrives on the wire.
func TestProxyAttachesUpstreamCredential(t *testing.T) {
	t.Parallel()

	up := newFakeTLSUpstream(t)

	app := newProxyApp(t, &config.Config{
		Upstreams: []config.Upstream{{
			Name: "workers",
			Listen: config.ListenConfig{
				HostPort: up.addr,
				TLS: &config.TLSConfig{
					CA:   up.caFile,
					Cert: up.clientCertFile,
					Key:  up.clientKeyFile,
					// The fake upstream's leaf certificate (from
					// GenerateMTLSCerts) advertises CN/DNSNames "localhost",
					// which does not match the 127.0.0.1 dial address.
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

	conn := dialUnix(t, up.addr)
	defer func() { _ = conn.Close() }()

	_, err := workflowservice.NewWorkflowServiceClient(conn).GetSystemInfo(
		startCtx, &workflowservice.GetSystemInfoRequest{}, grpc.WaitForReady(true),
	)
	require.NoError(t, err)

	got := up.svc.received()
	require.Equal(t, []string{"Bearer k3y"}, got.Get("authorization"),
		"expected the configured static credential to reach the upstream")
}
