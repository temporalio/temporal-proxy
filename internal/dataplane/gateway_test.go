package dataplane_test

import (
	"context"
	"net"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	prometheustest "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/temporalio/temporal-proxy/internal/auth"
	"github.com/temporalio/temporal-proxy/internal/config"
	"github.com/temporalio/temporal-proxy/internal/dataplane"
	"github.com/temporalio/temporal-proxy/internal/metrics"
	"github.com/temporalio/temporal-proxy/internal/services"
)

// echoMethod is the method the stand-in upstream in this file answers.
// AllowedServices only admits names Config.Validate recognizes, so the
// stand-in has to register under a real, known service rather than a
// fictional one.
const echoMethod = "/" + services.WorkflowService + "/GetSystemInfo"

// echoDesc is a stand-in gRPC service, registered under a real service name so
// the allowlist admits it. Its handler answers a WorkflowService method with a
// HealthCheckResponse, and that mismatch is the point: the exchange completes
// only if the gateway's codec moves bytes without parsing them against either
// descriptor. dataplanetest.Upstream answers the same method for real, which is
// the right fake elsewhere but cannot prove that.
var echoDesc = grpc.ServiceDesc{
	ServiceName: services.WorkflowService,
	HandlerType: (*any)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "GetSystemInfo",
			Handler: func(_ any, _ context.Context, dec func(any) error, _ grpc.UnaryServerInterceptor) (any, error) {
				in := new(grpc_health_v1.HealthCheckRequest)
				if err := dec(in); err != nil {
					return nil, err
				}

				return &grpc_health_v1.HealthCheckResponse{Status: grpc_health_v1.HealthCheckResponse_SERVING}, nil
			},
		},
	},
}

// TestGatewayForwardsAnAllowedUnregisteredMethod drives a request through the
// real gateway, over TCP, for a method the gateway itself never registers.
// Reaching a correct response proves the router's pass-through codec and
// forwarding handler are actually wired onto the server, not merely that a
// [Dataplane] and its upstream both exist.
func TestGatewayForwardsAnAllowedUnregisteredMethod(t *testing.T) {
	t.Parallel()

	cfg := testConfig()
	cfg.AllowedServices = config.Services{services.WorkflowService}
	cfg.Upstreams[0].Listen.HostPort = serveEcho(t)

	dp := startPlane(t, newTestDeps(t, cfg))

	conn := dialGateway(t, dp)
	resp := new(grpc_health_v1.HealthCheckResponse)
	require.NoError(t, conn.Invoke(t.Context(), echoMethod, &grpc_health_v1.HealthCheckRequest{}, resp))
	require.Equal(t, grpc_health_v1.HealthCheckResponse_SERVING, resp.GetStatus())
}

// TestGatewayRecordsRequestMetrics proves the server's metrics stream
// interceptor covers every request the router's unknown-service handler
// answers, including one the Gate rejects before any upstream work.
func TestGatewayRecordsRequestMetrics(t *testing.T) {
	t.Parallel()

	reg := prometheus.NewRegistry()

	cfg := liveConfig(t)
	cfg.AllowedServices = config.Services{services.WorkflowService}

	d := newTestDeps(t, cfg)
	d.metrics = metrics.New("tmprl_proxy", promauto.With(reg))

	dp := startPlane(t, d)

	conn := dialGateway(t, dp)
	err := conn.Invoke(
		t.Context(),
		"/some.other.v1.Service/Method",
		&grpc_health_v1.HealthCheckRequest{},
		new(grpc_health_v1.HealthCheckResponse),
	)
	require.Error(t, err)
	require.Equal(t, codes.Unimplemented, status.Code(err), "the Gate rejects a service outside AllowedServices")

	n, err := prometheustest.GatherAndCount(reg, "tmprl_proxy_server_requests_total")
	require.NoError(t, err)
	require.Equal(t, 1, n)
}

// TestGatewayEndToEndAuth proves the inbound authenticator and the router's
// forwarding handler compose on a real gateway: only a request carrying the
// configured static token reaches the stand-in upstream and gets a genuine
// response back.
func TestGatewayEndToEndAuth(t *testing.T) {
	t.Parallel()

	authenticator, err := auth.NewStaticTokenAuthenticator("secret", "", "")
	require.NoError(t, err)

	cfg := testConfig()
	cfg.AllowedServices = config.Services{services.WorkflowService}
	cfg.Upstreams[0].Listen.HostPort = serveEcho(t)

	d := newTestDeps(t, cfg)
	d.auth = authenticator

	dp := startPlane(t, d)
	conn := dialGateway(t, dp)

	invoke := func(ctx context.Context) (*grpc_health_v1.HealthCheckResponse, error) {
		resp := new(grpc_health_v1.HealthCheckResponse)
		err := conn.Invoke(ctx, echoMethod, &grpc_health_v1.HealthCheckRequest{}, resp)
		return resp, err
	}

	tests := []struct {
		name     string
		header   string // "" omits the authorization header entirely
		wantCode codes.Code
	}{
		{"correct static token succeeds", "Bearer secret", codes.OK},
		{"wrong token is rejected", "Bearer wrong", codes.Unauthenticated},
		{"missing token is rejected", "", codes.Unauthenticated},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			if tt.header != "" {
				ctx = metadata.AppendToOutgoingContext(ctx, "authorization", tt.header)
			}

			resp, err := invoke(ctx)
			require.Equal(t, tt.wantCode, status.Code(err))
			if tt.wantCode == codes.OK {
				require.NoError(t, err)
				require.Equal(t, grpc_health_v1.HealthCheckResponse_SERVING, resp.GetStatus())
			}
		})
	}
}

// serveEcho starts a plaintext gRPC server hosting echoDesc and returns its
// address for use as an upstream hostPort.
func serveEcho(t *testing.T) string {
	t.Helper()

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	svr := grpc.NewServer()
	svr.RegisterService(&echoDesc, nil)
	go func() { _ = svr.Serve(lis) }()
	t.Cleanup(svr.Stop)

	return lis.Addr().String()
}

// dialGateway dials the plane's inbound gateway the way a real client would:
// plaintext, since these tests never configure inbound TLS.
func dialGateway(t *testing.T, dp *dataplane.Dataplane) *grpc.ClientConn {
	t.Helper()

	conn, err := grpc.NewClient(dp.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })

	return conn
}
