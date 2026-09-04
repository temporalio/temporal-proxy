package translation

import (
	"context"
	"net"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	workflowservice "go.temporal.io/api/workflowservice/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// systemInfoService is a fake upstream serving the method the test translation
// substitutes onto, recording the metadata it was called with.
type systemInfoService struct {
	workflowservice.UnimplementedWorkflowServiceServer

	lis     net.Listener
	version string

	// mu guards md, which the serving goroutine writes while the test reads.
	mu sync.Mutex
	md metadata.MD
}

func testRegistry(t *testing.T, opts ...func(*Translation)) *Registry {
	t.Helper()

	tr := Adapt(fromMethod, toMethod, okRequest, okResponse)
	for _, opt := range opts {
		opt(tr)
	}

	r, err := NewRegistry(tr)
	require.NoError(t, err)

	return r
}

func TestUnaryClientInterceptorPassesUntranslatedMethodsThrough(t *testing.T) {
	t.Parallel()

	var gotMethod string
	invoker := func(_ context.Context, method string, _, _ any, _ *grpc.ClientConn, _ ...grpc.CallOption) error {
		gotMethod = method
		return nil
	}

	const other = "/temporal.api.workflowservice.v1.WorkflowService/ListNamespaces"
	err := UnaryClientInterceptor(testRegistry(t))(
		t.Context(), other,
		&workflowservice.ListNamespacesRequest{}, &workflowservice.ListNamespacesResponse{}, nil, invoker,
	)
	require.NoError(t, err)
	require.Equal(t, other, gotMethod)
}

func TestUnaryClientInterceptorSubstitutesTheUpstreamCall(t *testing.T) {
	t.Parallel()

	var (
		gotMethod string
		gotReq    *workflowservice.GetSystemInfoRequest
	)

	invoker := func(_ context.Context, method string, req, reply any, _ *grpc.ClientConn, _ ...grpc.CallOption) error {
		gotMethod = method
		gotReq = mustProto[*workflowservice.GetSystemInfoRequest](t, req)
		mustProto[*workflowservice.GetSystemInfoResponse](t, reply).ServerVersion = "1.2.3"

		return nil
	}

	reply := &workflowservice.DescribeNamespaceResponse{}
	err := UnaryClientInterceptor(testRegistry(t))(
		t.Context(), fromMethod,
		&workflowservice.DescribeNamespaceRequest{Namespace: "payments"}, reply, nil, invoker,
	)
	require.NoError(t, err)

	require.Equal(t, toMethod, gotMethod, "the upstream sees the substituted method")
	require.NotNil(t, gotReq, "and the converted request")
	require.Equal(t, "payments@1.2.3", reply.GetNamespaceInfo().GetName())
}

func TestUnaryClientInterceptorReturnsTheUpstreamError(t *testing.T) {
	t.Parallel()

	want := status.Error(codes.PermissionDenied, "no")
	invoker := func(context.Context, string, any, any, *grpc.ClientConn, ...grpc.CallOption) error {
		return want
	}

	err := UnaryClientInterceptor(testRegistry(t))(
		t.Context(), fromMethod,
		&workflowservice.DescribeNamespaceRequest{}, &workflowservice.DescribeNamespaceResponse{}, nil, invoker,
	)
	require.Equal(t, want, err, "the caller sees the upstream's status, untranslated")
}

func TestUnaryClientInterceptorReportsConversionFailureAsInternal(t *testing.T) {
	t.Parallel()

	// A reply of the wrong type for the registered translation: the request
	// converts, the call succeeds, and folding the reply back fails.
	invoker := func(context.Context, string, any, any, *grpc.ClientConn, ...grpc.CallOption) error {
		return nil
	}

	err := UnaryClientInterceptor(testRegistry(t))(
		t.Context(), fromMethod,
		&workflowservice.DescribeNamespaceRequest{}, &workflowservice.ListNamespacesResponse{}, nil, invoker,
	)
	require.Equal(t, codes.Internal, status.Code(err))
}

func TestUnaryClientInterceptorForwardsNonProtoCallsUnchanged(t *testing.T) {
	t.Parallel()

	var gotMethod string
	invoker := func(_ context.Context, method string, _, _ any, _ *grpc.ClientConn, _ ...grpc.CallOption) error {
		gotMethod = method
		return nil
	}

	err := UnaryClientInterceptor(testRegistry(t))(t.Context(), fromMethod, "req", "reply", nil, invoker)
	require.NoError(t, err)
	require.Equal(t, fromMethod, gotMethod, "nothing to convert, so nothing is substituted")
}

func TestUnaryClientInterceptorStampsHeadersOnTheSubstitutedCallOnly(t *testing.T) {
	t.Parallel()

	r := testRegistry(t, func(tr *Translation) { tr.WithHeader("x-api-version", "v1") })

	var gotMD metadata.MD
	invoker := func(ctx context.Context, _ string, _, _ any, _ *grpc.ClientConn, _ ...grpc.CallOption) error {
		gotMD, _ = metadata.FromOutgoingContext(ctx)
		return nil
	}

	err := UnaryClientInterceptor(r)(
		t.Context(), fromMethod,
		&workflowservice.DescribeNamespaceRequest{}, &workflowservice.DescribeNamespaceResponse{}, nil, invoker,
	)
	require.NoError(t, err)
	require.Equal(t, []string{"v1"}, gotMD.Get("x-api-version"))

	// An untranslated call keeps whatever the caller sent.
	ctx := metadata.NewOutgoingContext(t.Context(), metadata.Pairs("x-api-version", "caller"))
	err = UnaryClientInterceptor(r)(
		ctx, "/pkg.Other/Untranslated",
		&workflowservice.DescribeNamespaceRequest{}, &workflowservice.DescribeNamespaceResponse{}, nil, invoker,
	)
	require.NoError(t, err)
	require.Equal(t, []string{"caller"}, gotMD.Get("x-api-version"))
}

func TestViaSendsTheSubstitutedCallElsewhere(t *testing.T) {
	t.Parallel()

	// Via is what lets a translation answer over a connection other than the one
	// it is installed on, for an upstream method the original connection's service
	// does not serve.
	elsewhere := newSystemInfoService(t, "9.9.9")

	cc, err := grpc.NewClient(elsewhere.addr(t), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = cc.Close() })

	var chained bool
	invoker := func(context.Context, string, any, any, *grpc.ClientConn, ...grpc.CallOption) error {
		chained = true
		return nil
	}

	reply := &workflowservice.DescribeNamespaceResponse{}
	err = UnaryClientInterceptor(testRegistry(t), Via(cc))(
		t.Context(), fromMethod,
		&workflowservice.DescribeNamespaceRequest{Namespace: "payments"}, reply, nil, invoker,
	)
	require.NoError(t, err)

	require.False(t, chained, "the call must leave the chain rather than continue down it")
	require.Equal(t, "payments@9.9.9", reply.GetNamespaceInfo().GetName())
}

func TestDialOptionsTranslateOverARealConnection(t *testing.T) {
	t.Parallel()

	upstream := newSystemInfoService(t, "1.2.3")

	dialOpts := append(
		[]grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())},
		DialOptions(testRegistry(t, func(tr *Translation) { tr.WithHeader("x-api-version", "v1") }))...,
	)
	cc, err := grpc.NewClient(upstream.addr(t), dialOpts...)
	require.NoError(t, err)
	t.Cleanup(func() { _ = cc.Close() })

	// Invoke the inbound method by name, the way the reflective forwarder does:
	// the interceptor is what turns it into the substituted call.
	reply := &workflowservice.DescribeNamespaceResponse{}
	err = cc.Invoke(t.Context(), fromMethod, &workflowservice.DescribeNamespaceRequest{Namespace: "payments"}, reply)
	require.NoError(t, err)

	require.Equal(t, "payments@1.2.3", reply.GetNamespaceInfo().GetName())
	require.Equal(t, []string{"v1"}, upstream.metadata().Get("x-api-version"), "the header reached the wire")
}

func (s *systemInfoService) GetSystemInfo(
	ctx context.Context, _ *workflowservice.GetSystemInfoRequest,
) (*workflowservice.GetSystemInfoResponse, error) {
	md, _ := metadata.FromIncomingContext(ctx)

	s.mu.Lock()
	defer s.mu.Unlock()

	s.md = md

	return &workflowservice.GetSystemInfoResponse{ServerVersion: s.version}, nil
}

// newSystemInfoService starts the fake on a loopback port and stops it when the
// test ends.
func newSystemInfoService(t *testing.T, version string) *systemInfoService {
	t.Helper()

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	svc := &systemInfoService{lis: lis, version: version}

	svr := grpc.NewServer()
	workflowservice.RegisterWorkflowServiceServer(svr, svc)
	go func() { _ = svr.Serve(lis) }()
	t.Cleanup(svr.Stop)

	return svc
}

func (s *systemInfoService) addr(t *testing.T) string {
	t.Helper()
	return s.lis.Addr().String()
}

func (s *systemInfoService) metadata() metadata.MD {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.md.Copy()
}
