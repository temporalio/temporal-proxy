package e2e

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/api/enums/v1"
	"go.temporal.io/api/operatorservice/v1"
	"go.temporal.io/api/workflowservice/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"
	reflectionpb "google.golang.org/grpc/reflection/grpc_reflection_v1"
	"google.golang.org/grpc/status"

	"github.com/temporalio/temporal-proxy/internal/config"
	"github.com/temporalio/temporal-proxy/internal/services"
)

// fakeFrontend answers the OperatorService and WorkflowService methods these
// tests drive, recording the namespace it saw so a case can prove the request
// actually reached it rather than being satisfied by the gateway. Unlike
// newFakeUpstream and newPlaintextUpstream elsewhere in this package, this
// fake must expose both gRPC services and reflection: the existing builders
// each register only WorkflowService with no reflection, so neither can stand
// in for the OperatorService and reflection tests without growing those same
// additions -- at which point it stops being either of them. It does not
// register a health server: the proxy answers health checks from its own
// inbound server rather than forwarding them, so an upstream health service
// would be inert setup.
type fakeFrontend struct {
	operatorservice.UnimplementedOperatorServiceServer
	workflowservice.UnimplementedWorkflowServiceServer

	gotNamespace string
}

// TestEndToEndOperatorServiceReachesUpstream proves that with no
// allowedServices configured, the default set admits OperatorService end to
// end: the call must actually reach the fake upstream (recording the
// namespace it saw), not merely return without error. Before the reflective
// forwarder existed, OperatorService had no generated handler in the inbound
// server at all, so this call would fail with Unimplemented; this test would
// have caught that regression directly.
func TestEndToEndOperatorServiceReachesUpstream(t *testing.T) {
	t.Parallel()

	up, addr := newFakeFrontend(t)
	conn := startProxy(t, addr, nil) // default list

	resp, err := operatorservice.NewOperatorServiceClient(conn).ListSearchAttributes(
		t.Context(),
		&operatorservice.ListSearchAttributesRequest{Namespace: "ns-1"},
		grpc.WaitForReady(true),
	)
	require.NoError(t, err, "OperatorService must be forwarded by default")
	require.Contains(t, resp.GetCustomAttributes(), "attr")
	require.Equal(t, "ns-1", up.gotNamespace)

	_, err = workflowservice.NewWorkflowServiceClient(conn).GetSystemInfo(
		t.Context(), &workflowservice.GetSystemInfoRequest{},
	)
	require.NoError(t, err, "WorkflowService must keep working")
}

// TestEndToEndNamedHealthChecks proves the proxy answers named health checks
// the way the Temporal Go SDK's CheckHealth does: by exact service name, one
// registration per exposed service, and NotFound for anything not exposed.
// Before per-service health statuses existed, a single blanket status could
// make this pass for services the proxy does not actually expose; asserting
// NotFound for an unexposed name is what would catch that regression.
func TestEndToEndNamedHealthChecks(t *testing.T) {
	t.Parallel()

	_, addr := newFakeFrontend(t)
	conn := startProxy(t, addr, nil)

	client := grpc_health_v1.NewHealthClient(conn)
	for _, name := range services.Default() {
		resp, err := client.Check(t.Context(), &grpc_health_v1.HealthCheckRequest{Service: name})
		require.NoError(t, err, "the SDK's CheckHealth asks for %s by name", name)
		require.Equal(t, grpc_health_v1.HealthCheckResponse_SERVING, resp.GetStatus())
	}

	_, err := client.Check(t.Context(), &grpc_health_v1.HealthCheckRequest{
		Service: "temporal.server.api.adminservice.v1.AdminService",
	})
	require.Equal(t, codes.NotFound, status.Code(err))
}

// TestEndToEndReflectionIsOptIn proves reflection is refused by default and
// works once listed, and that the working case actually round-trips through
// both hops (inbound server to per-upstream proxy to the fake frontend's own
// reflection service) rather than being answered locally. Before the
// reflective forwarder's streaming path existed, reflection had no forwarding
// route at all, so the "works when listed" subtest would have failed outright
// rather than merely returning an empty list.
//
// Each subtest gets its own fake frontend rather than sharing one address:
// the proxy's unix socket path is derived deterministically from the upstream
// host:port (internal/transport/socket.UnixPath), so two parallel subtests
// dialing the same upstream address race to bind the same socket file and one
// fails with "file exists". A shared address made this test flaky.
func TestEndToEndReflectionIsOptIn(t *testing.T) {
	t.Parallel()

	t.Run("refused by default", func(t *testing.T) {
		t.Parallel()

		_, addr := newFakeFrontend(t)
		conn := startProxy(t, addr, nil)
		stream, err := reflectionpb.NewServerReflectionClient(conn).ServerReflectionInfo(t.Context())
		require.NoError(t, err)

		// The gate refuses the stream before any frame is exchanged, so the
		// server can close it before this Send is processed; grpc-go then
		// surfaces that as io.EOF from Send rather than the real status, which
		// Recv always carries. Asserting on Send's error here would make the
		// test flake on that race.
		_ = stream.Send(&reflectionpb.ServerReflectionRequest{
			MessageRequest: &reflectionpb.ServerReflectionRequest_ListServices{ListServices: ""},
		})

		_, err = stream.Recv()
		require.Equal(t, codes.Unimplemented, status.Code(err))
		require.Contains(t, status.Convert(err).Message(), "unknown service "+services.Reflection,
			"the refusal must name reflection, not some other Unimplemented in the stack")
	})

	t.Run("works when listed", func(t *testing.T) {
		t.Parallel()

		_, addr := newFakeFrontend(t)
		conn := startProxy(t, addr, append(services.Default(), services.Reflection))
		stream, err := reflectionpb.NewServerReflectionClient(conn).ServerReflectionInfo(t.Context())
		require.NoError(t, err)
		require.NoError(t, stream.Send(&reflectionpb.ServerReflectionRequest{
			MessageRequest: &reflectionpb.ServerReflectionRequest_ListServices{ListServices: ""},
		}))

		resp, err := stream.Recv()
		require.NoError(t, err)
		require.NotEmpty(t, resp.GetListServicesResponse().GetService(),
			"the upstream's service list must arrive through both hops")
	})
}

// TestEndToEndDisallowedServiceLooksMissing proves a service the proxy does
// not expose is refused with the same shape a frontend that never had it
// would produce, and -- the part a mere status-code check would miss -- that
// the refusal happens before the upstream hop; the fake upstream must never
// observe the call. Before the gateway's admission gate existed, an
// unconfigured service reached the forwarder and was rejected downstream (or
// worse, forwarded), so asserting the upstream saw nothing is what would
// catch a regression that moved the rejection point.
func TestEndToEndDisallowedServiceLooksMissing(t *testing.T) {
	t.Parallel()

	up, addr := newFakeFrontend(t)
	conn := startProxy(t, addr, []string{services.WorkflowService})

	_, err := operatorservice.NewOperatorServiceClient(conn).ListSearchAttributes(
		t.Context(),
		&operatorservice.ListSearchAttributesRequest{Namespace: "ns-1"},
		grpc.WaitForReady(true),
	)
	require.Equal(t, codes.Unimplemented, status.Code(err))
	require.Contains(t, status.Convert(err).Message(), "unknown service "+services.OperatorService,
		"a refused service must be indistinguishable from a frontend that lacks it")
	require.Empty(t, up.gotNamespace, "the upstream must never see a call the gate refused")
}

// TestEndToEndOperatorServiceNamespaceIsTranslated proves namespace
// translation composes with the reflective forwarder: an OperatorService
// request carrying the local namespace name must reach the fake upstream with
// the translated remote name, not the local one the caller sent. Without this,
// the branch's central justification for decoding requests rather than
// relaying raw frames -- that a raw relay would silently skip translation --
// is proven only in disjoint pieces: the forwarder's own unit tests dial the
// upstream with no translation dial options installed, and every other
// end-to-end case in this file configures no namespace rules at all.
func TestEndToEndOperatorServiceNamespaceIsTranslated(t *testing.T) {
	t.Parallel()

	up, addr := newFakeFrontend(t)

	inboundAddr := freeTCPAddr(t)
	app := newFullApp(t, &config.Config{
		Listen:  config.ListenConfig{HostPort: inboundAddr},
		Routing: config.Routing{DefaultUpstream: "up"},
		Upstreams: []config.Upstream{{
			Name:   "up",
			Listen: config.ListenConfig{HostPort: addr},
			Namespaces: config.NamespaceConfig{
				Rules: config.NamespaceRules{Prefix: "remote-"},
			},
		}},
	})
	require.NoError(t, app.Err())

	startCtx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	t.Cleanup(cancel)
	require.NoError(t, app.Start(startCtx))
	t.Cleanup(func() {
		stopCtx, stopCancel := context.WithTimeout(context.WithoutCancel(t.Context()), 5*time.Second)
		defer stopCancel()
		_ = app.Stop(stopCtx)
	})

	conn := dialInbound(t, inboundAddr)

	_, err := operatorservice.NewOperatorServiceClient(conn).ListSearchAttributes(
		t.Context(),
		&operatorservice.ListSearchAttributesRequest{Namespace: "ns-1"},
		grpc.WaitForReady(true),
	)
	require.NoError(t, err)
	require.Equal(t, "remote-ns-1", up.gotNamespace,
		"the upstream must see the translated remote namespace, not the local one the caller sent")
}

func (f *fakeFrontend) ListSearchAttributes(
	_ context.Context, req *operatorservice.ListSearchAttributesRequest,
) (*operatorservice.ListSearchAttributesResponse, error) {
	f.gotNamespace = req.GetNamespace()

	return &operatorservice.ListSearchAttributesResponse{
		CustomAttributes: map[string]enums.IndexedValueType{"attr": 0},
	}, nil
}

func (f *fakeFrontend) GetSystemInfo(
	context.Context, *workflowservice.GetSystemInfoRequest,
) (*workflowservice.GetSystemInfoResponse, error) {
	return &workflowservice.GetSystemInfoResponse{}, nil
}

// newFakeFrontend stands up an upstream shaped like a real Temporal frontend:
// both gRPC services plus reflection.
func newFakeFrontend(t *testing.T) (*fakeFrontend, string) {
	t.Helper()

	svc := &fakeFrontend{}
	srv := grpc.NewServer()
	workflowservice.RegisterWorkflowServiceServer(srv, svc)
	operatorservice.RegisterOperatorServiceServer(srv, svc)
	reflection.Register(srv)

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)

	return svc, lis.Addr().String()
}

// startProxy boots the full stack in front of upstreamAddr with the given
// allowed service list and returns a client conn to its inbound address.
func startProxy(t *testing.T, upstreamAddr string, allowed []string) *grpc.ClientConn {
	t.Helper()

	inboundAddr := freeTCPAddr(t)
	app := newFullApp(t, &config.Config{
		Listen:          config.ListenConfig{HostPort: inboundAddr},
		AllowedServices: allowed,
		Routing:         config.Routing{DefaultUpstream: "up"},
		Upstreams: []config.Upstream{{
			Name:   "up",
			Listen: config.ListenConfig{HostPort: upstreamAddr},
		}},
	})
	require.NoError(t, app.Err())

	startCtx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	t.Cleanup(cancel)
	require.NoError(t, app.Start(startCtx))
	t.Cleanup(func() {
		stopCtx, stopCancel := context.WithTimeout(context.WithoutCancel(t.Context()), 5*time.Second)
		defer stopCancel()
		_ = app.Stop(stopCtx)
	})

	return dialInbound(t, inboundAddr)
}
