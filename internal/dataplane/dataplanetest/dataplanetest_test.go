package dataplanetest_test

import (
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/api/workflowservice/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/temporalio/temporal-proxy/internal/config"
	"github.com/temporalio/temporal-proxy/internal/dataplane/dataplanetest"
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

// TestStartRejectsConfigItCannotHonour guards the direct path against silently
// passing a test whose subject it never wired up.
func TestStartRejectsConfigItCannotHonour(t *testing.T) {
	t.Parallel()

	cases := map[string]func(*config.Config){
		"auth": func(cfg *config.Config) {
			cfg.Auth = &config.AuthConfig{StaticToken: &config.StaticTokenConfig{Token: "s3cret"}}
		},
		"encryption": func(cfg *config.Config) {
			cfg.Encryption = config.Encryption{Default: &config.KeyPolicy{Duration: time.Hour}}
		},
	}

	for name, configure := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			cfg := dataplanetest.Config(dataplanetest.NewUpstream(t))
			configure(cfg)

			// Start fails its argument, which unwinds the calling goroutine, so
			// the attempt runs on one of its own. It rejects before building
			// anything, so the throwaway T is never asked to clean up.
			fake := new(testing.T)
			done := make(chan struct{})
			go func() {
				defer close(done)

				dataplanetest.Start(fake, cfg)
			}()
			<-done

			require.True(t, fake.Failed(), "Start must reject a config only StartApp can honour")
		})
	}
}

// TestStartAppBuildsConfigDrivenCollaborators proves the fx path assembles the
// modules Start cannot: the inbound authenticator here comes from the Auth
// block, so a wrong token is rejected before the request is routed anywhere.
func TestStartAppBuildsConfigDrivenCollaborators(t *testing.T) {
	t.Parallel()

	up := dataplanetest.NewUpstream(t)
	cfg := dataplanetest.Config(up)
	cfg.Auth = &config.AuthConfig{StaticToken: &config.StaticTokenConfig{Token: "s3cret"}}

	f := dataplanetest.StartApp(t, cfg)

	ctx := metadata.AppendToOutgoingContext(f.Context(), "authorization", "Bearer s3cret")
	_, err := f.Client().GetSystemInfo(ctx, &workflowservice.GetSystemInfoRequest{}, grpc.WaitForReady(true))
	require.NoError(t, err)

	bad := metadata.AppendToOutgoingContext(f.Context(), "authorization", "Bearer nope")
	_, err = f.Client().GetSystemInfo(bad, &workflowservice.GetSystemInfoRequest{})
	require.Equal(t, codes.Unauthenticated, status.Code(err))
}

func TestUpstreamConnReachesTheProxySocketDirectly(t *testing.T) {
	t.Parallel()

	up := dataplanetest.NewUpstream(t)
	f := dataplanetest.Start(t, dataplanetest.Config(up))

	// The per-upstream socket is a supported access path for local workers, so
	// it must serve without going through the gateway.
	client := workflowservice.NewWorkflowServiceClient(f.UpstreamConn(dataplanetest.DefaultUpstream))

	_, err := client.GetSystemInfo(f.Context(), &workflowservice.GetSystemInfoRequest{}, grpc.WaitForReady(true))
	require.NoError(t, err)
	require.NotNil(t, up.Metadata())
}
