package dataplanetest_test

import (
	"net"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/api/workflowservice/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"go.temporal.io/proxy/internal/auth"
	"go.temporal.io/proxy/internal/config"
	"go.temporal.io/proxy/internal/dataplane/dataplanetest"
)

func TestStartForwardsToTheFakeUpstream(t *testing.T) {
	t.Parallel()

	up := dataplanetest.NewUpstream(t)
	f := dataplanetest.Start(t, dataplanetest.Config(up))

	_, err := f.Client().GetSystemInfo(
		f.Context(), &workflowservice.GetSystemInfoRequest{}, grpc.WaitForReady(true),
	)
	require.NoError(t, err)
	require.NotNil(t, up.Metadata(), "the request must have reached the fake upstream")
}

// TestStartReportsTheKernelAssignedGatewayPort proves nobody has to pick a
// port: the fixture binds an ephemeral one and reports what it got, so two
// planes running in parallel cannot collide, and there is no window between
// choosing an address and binding it.
func TestStartReportsTheKernelAssignedGatewayPort(t *testing.T) {
	t.Parallel()

	first := dataplanetest.Start(t, dataplanetest.Config(dataplanetest.NewUpstream(t)))
	second := dataplanetest.Start(t, dataplanetest.Config(dataplanetest.NewUpstream(t)))

	_, port, err := net.SplitHostPort(first.Addr())
	require.NoError(t, err)
	require.NotEqual(t, "0", port, "Addr reports the port the kernel picked, not the one requested")

	require.NotEqual(t, first.Addr(), second.Addr())
}

// TestStartBuildsConfigDrivenCollaborators proves the fixture runs the
// production graph: the inbound authenticator here comes from the Auth block,
// so a wrong token is rejected before the request is routed anywhere.
func TestStartBuildsConfigDrivenCollaborators(t *testing.T) {
	t.Parallel()

	up := dataplanetest.NewUpstream(t)
	cfg := dataplanetest.Config(up)
	cfg.Auth = &config.AuthConfig{StaticToken: &config.StaticTokenConfig{Token: "s3cret"}}

	f := dataplanetest.Start(t, cfg)

	ctx := metadata.AppendToOutgoingContext(f.Context(), "authorization", "Bearer s3cret")
	_, err := f.Client().GetSystemInfo(ctx, &workflowservice.GetSystemInfoRequest{}, grpc.WaitForReady(true))
	require.NoError(t, err)

	bad := metadata.AppendToOutgoingContext(f.Context(), "authorization", "Bearer nope")
	_, err = f.Client().GetSystemInfo(bad, &workflowservice.GetSystemInfoRequest{})
	require.Equal(t, codes.Unauthenticated, status.Code(err))
}

// TestWithAuthReplacesTheConfiguredAuthenticator proves an injected
// authenticator wins over the config's Auth block, so a test setting both never
// silently runs the configured one.
func TestWithAuthReplacesTheConfiguredAuthenticator(t *testing.T) {
	t.Parallel()

	up := dataplanetest.NewUpstream(t)
	cfg := dataplanetest.Config(up)
	cfg.Auth = &config.AuthConfig{StaticToken: &config.StaticTokenConfig{Token: "s3cret"}}

	f := dataplanetest.Start(t, cfg, dataplanetest.WithAuth(auth.AdmitAll()))

	_, err := f.Client().GetSystemInfo(f.Context(), &workflowservice.GetSystemInfoRequest{}, grpc.WaitForReady(true))
	require.NoError(t, err)
}
