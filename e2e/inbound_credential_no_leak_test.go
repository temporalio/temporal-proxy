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

	up := dataplanetest.NewTLSUpstream(t)

	cfg := dataplanetest.Config(up)
	// Inbound auth consumes a custom header, distinct from the outbound credential's.
	cfg.Auth = &config.AuthConfig{
		StaticToken: &config.StaticTokenConfig{Token: "worker-secret", Header: "x-worker-auth"},
	}
	// The outbound credential's header defaults to authorization.
	cfg.Upstreams[0].Credentials = &config.CredentialConfig{
		Static: &config.StaticCredentialConfig{APIKey: "k3y"},
	}

	f := dataplanetest.StartApp(t, cfg)

	ctx := metadata.AppendToOutgoingContext(f.Context(), "x-worker-auth", "Bearer worker-secret")
	_, err := f.Client().GetSystemInfo(ctx, &workflowservice.GetSystemInfoRequest{}, grpc.WaitForReady(true))
	require.NoError(t, err)

	got := up.Metadata()
	require.Empty(t, got.Get("x-worker-auth"), "the worker credential (on its own header) must not leak upstream")
	require.Equal(t, []string{"Bearer k3y"}, got.Get("authorization"), "the upstream must see exactly the API key")
}
