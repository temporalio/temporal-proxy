package proxy_test

import (
	"context"
	"net"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/api/enums/v1"
	"go.temporal.io/api/operatorservice/v1"
	"go.temporal.io/api/workflowservice/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	"github.com/temporalio/temporal-proxy/internal/proxy"
	"github.com/temporalio/temporal-proxy/internal/services"
)

// recordingUpstream answers the two unary methods the forwarder tests drive and
// records the metadata each arrived with.
type recordingUpstream struct {
	operatorservice.UnimplementedOperatorServiceServer
	workflowservice.UnimplementedWorkflowServiceServer

	md metadata.MD
}

func (u *recordingUpstream) ListSearchAttributes(
	ctx context.Context, req *operatorservice.ListSearchAttributesRequest,
) (*operatorservice.ListSearchAttributesResponse, error) {
	u.md, _ = metadata.FromIncomingContext(ctx)

	return &operatorservice.ListSearchAttributesResponse{
		CustomAttributes: map[string]enums.IndexedValueType{req.GetNamespace(): 0},
	}, nil
}

func (u *recordingUpstream) GetSystemInfo(
	ctx context.Context, _ *workflowservice.GetSystemInfoRequest,
) (*workflowservice.GetSystemInfoResponse, error) {
	u.md, _ = metadata.FromIncomingContext(ctx)
	return &workflowservice.GetSystemInfoResponse{}, nil
}

// dialBuf returns a client conn to a bufconn listener. Extra opts are appended
// after the transport options, so a caller can add interceptors without
// duplicating the dialing boilerplate.
func dialBuf(t *testing.T, lis *bufconn.Listener, opts ...grpc.DialOption) *grpc.ClientConn {
	t.Helper()

	dialOpts := append([]grpc.DialOption{
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	}, opts...)

	conn, err := grpc.NewClient("passthrough:///bufnet", dialOpts...)
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })

	return conn
}

// newForwarderConn stands up a fake upstream, then a server whose only handler
// is the forwarder admitting both services the fixture upstream implements,
// and returns a client conn pointed at that server. Tests exercising the
// admission gate itself use newForwarderConnAllowing directly.
func newForwarderConn(t *testing.T, upstream *recordingUpstream) *grpc.ClientConn {
	t.Helper()

	return newForwarderConnAllowing(t, upstream, []string{services.WorkflowService, services.OperatorService})
}

// newForwarderConnAllowing is newForwarderConn with an explicit allowed set, so
// a case can prove the forwarder refuses a service outside it even though the
// fixture upstream still implements it.
func newForwarderConnAllowing(t *testing.T, upstream *recordingUpstream, allowed []string) *grpc.ClientConn {
	t.Helper()

	upLis := bufconn.Listen(1024 * 1024)
	upSrv := grpc.NewServer()
	operatorservice.RegisterOperatorServiceServer(upSrv, upstream)
	workflowservice.RegisterWorkflowServiceServer(upSrv, upstream)
	go func() { _ = upSrv.Serve(upLis) }()
	t.Cleanup(upSrv.Stop)

	upConn := dialBuf(t, upLis)

	fwdLis := bufconn.Listen(1024 * 1024)
	fwdSrv := grpc.NewServer(grpc.UnknownServiceHandler(proxy.NewForwarder(upConn, allowed).Handle))
	go func() { _ = fwdSrv.Serve(fwdLis) }()
	t.Cleanup(fwdSrv.Stop)

	return dialBuf(t, fwdLis)
}

func TestForwarderCarriesUnaryCallsForAnyRegisteredService(t *testing.T) {
	t.Parallel()

	up := &recordingUpstream{}
	conn := newForwarderConn(t, up)

	resp, err := operatorservice.NewOperatorServiceClient(conn).ListSearchAttributes(
		t.Context(), &operatorservice.ListSearchAttributesRequest{Namespace: "ns-1"},
	)
	require.NoError(t, err)
	require.Contains(t, resp.GetCustomAttributes(), "ns-1",
		"the reply must round trip through the forwarder decoded, not empty")

	_, err = workflowservice.NewWorkflowServiceClient(conn).GetSystemInfo(
		t.Context(), &workflowservice.GetSystemInfoRequest{},
	)
	require.NoError(t, err, "the service the proxy already carried must keep working")
}

func TestForwarderCopiesIncomingMetadataUpstream(t *testing.T) {
	t.Parallel()

	up := &recordingUpstream{}
	conn := newForwarderConn(t, up)

	ctx := metadata.AppendToOutgoingContext(t.Context(), "x-custom", "kept")
	_, err := workflowservice.NewWorkflowServiceClient(conn).GetSystemInfo(
		ctx, &workflowservice.GetSystemInfoRequest{},
	)
	require.NoError(t, err)

	require.Equal(t, []string{"kept"}, up.md.Get("x-custom"),
		"templated upstreams and namespace translation depend on this copy")
	require.Len(t, up.md.Get("user-agent"), 1,
		"the caller's user-agent must not be forwarded on top of the proxy's own")
}

func TestForwarderRefusesUnresolvableMethod(t *testing.T) {
	t.Parallel()

	conn := newForwarderConn(t, &recordingUpstream{})

	err := conn.Invoke(t.Context(),
		"/temporal.api.workflowservice.v1.WorkflowService/MethodFromTheFuture",
		&workflowservice.GetSystemInfoRequest{}, &workflowservice.GetSystemInfoResponse{},
	)
	require.Equal(t, codes.Unimplemented, status.Code(err),
		"relaying an untypeable method raw would skip translation and encryption")
}

func TestForwarderPropagatesUpstreamError(t *testing.T) {
	t.Parallel()

	conn := newForwarderConn(t, &recordingUpstream{})

	// DeleteNamespace is not implemented by the fake upstream, which answers
	// Unimplemented; the status must arrive intact rather than as Internal.
	_, err := operatorservice.NewOperatorServiceClient(conn).DeleteNamespace(
		t.Context(), &operatorservice.DeleteNamespaceRequest{Namespace: "ns-1"},
	)
	require.Equal(t, codes.Unimplemented, status.Code(err))
}

// TestForwarderRefusesServiceOutsideAllowedSet proves the forwarder gates on
// its own admitted set rather than on whatever is merely linked in. The
// fixture upstream implements OperatorService and the forwarder can resolve
// its descriptors, but the forwarder here is only told to admit
// WorkflowService, so the call must be refused before it ever reaches the
// upstream. This is the scenario that matters: a caller dialing this proxy's
// unix socket directly, bypassing the gateway's own allowedServices gate
// entirely, must not reach a service the gateway would have refused.
func TestForwarderRefusesServiceOutsideAllowedSet(t *testing.T) {
	t.Parallel()

	up := &recordingUpstream{}
	conn := newForwarderConnAllowing(t, up, []string{services.WorkflowService})

	_, err := operatorservice.NewOperatorServiceClient(conn).ListSearchAttributes(
		t.Context(), &operatorservice.ListSearchAttributesRequest{Namespace: "ns-1"},
	)
	require.Equal(t, codes.Unimplemented, status.Code(err))
	require.Contains(t, status.Convert(err).Message(), "unknown service "+services.OperatorService,
		"a refused service must be indistinguishable from a socket that never linked it")
	require.Nil(t, up.md, "the upstream must never see a call the forwarder's own gate refused")
}
