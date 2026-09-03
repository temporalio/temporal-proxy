package translation

import (
	"context"
	"net"
	"testing"

	"github.com/stretchr/testify/require"
	workflowservice "go.temporal.io/api/workflowservice/v1"
	cloudservice "go.temporal.io/cloud-sdk/api/cloudservice/v1"
	cloudnamespace "go.temporal.io/cloud-sdk/api/namespace/v1"
	"go.temporal.io/cloud-sdk/api/resource/v1"
	"go.temporal.io/cloud-sdk/cloudclient"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// namespaceLister is a fake CloudService that answers GetNamespaces from a
// fixed page and records the request it was asked with.
type namespaceLister struct {
	cloudservice.UnimplementedCloudServiceServer

	got   *cloudservice.GetNamespacesRequest
	gotMD metadata.MD
	page  *cloudservice.GetNamespacesResponse
}

func TestUnaryClientInterceptorPassesUntranslatedMethodsThrough(t *testing.T) {
	t.Parallel()

	r, err := Default()
	require.NoError(t, err)

	var gotMethod string
	invoker := func(_ context.Context, method string, _, _ any, _ *grpc.ClientConn, _ ...grpc.CallOption) error {
		gotMethod = method
		return nil
	}

	req := &workflowservice.DescribeNamespaceRequest{Namespace: "ns"}
	reply := &workflowservice.DescribeNamespaceResponse{}
	err = UnaryClientInterceptor(r)(
		t.Context(), "/temporal.api.workflowservice.v1.WorkflowService/DescribeNamespace",
		req, reply, nil, invoker,
	)
	require.NoError(t, err)
	require.Equal(t, "/temporal.api.workflowservice.v1.WorkflowService/DescribeNamespace", gotMethod)
}

func TestUnaryClientInterceptorSubstitutesTheUpstreamCall(t *testing.T) {
	t.Parallel()

	r, err := Default()
	require.NoError(t, err)

	var (
		gotMethod string
		gotReq    *cloudservice.GetNamespacesRequest
	)

	invoker := func(_ context.Context, method string, req, reply any, _ *grpc.ClientConn, _ ...grpc.CallOption) error {
		gotMethod = method
		gotReq = mustProto[*cloudservice.GetNamespacesRequest](t, req)

		out := mustProto[*cloudservice.GetNamespacesResponse](t, reply)
		out.Namespaces = []*cloudnamespace.Namespace{activeNamespace("payments.a1b2c")}
		out.NextPageToken = "next"

		return nil
	}

	reply := &workflowservice.ListNamespacesResponse{}
	err = UnaryClientInterceptor(r)(
		t.Context(), listNamespacesMethod,
		&workflowservice.ListNamespacesRequest{PageSize: 7, NextPageToken: []byte("here")},
		reply, nil, invoker,
	)
	require.NoError(t, err)

	require.Equal(t, getNamespacesMethod, gotMethod, "the upstream sees the Cloud method")
	require.Equal(t, int32(7), gotReq.GetPageSize())
	require.Equal(t, "here", gotReq.GetPageToken())

	require.Len(t, reply.GetNamespaces(), 1)
	require.Equal(t, "payments.a1b2c", reply.GetNamespaces()[0].GetNamespaceInfo().GetName())
	require.Equal(t, []byte("next"), reply.GetNextPageToken())
}

func TestUnaryClientInterceptorReturnsTheUpstreamError(t *testing.T) {
	t.Parallel()

	r, err := Default()
	require.NoError(t, err)

	want := status.Error(codes.PermissionDenied, "no")
	invoker := func(context.Context, string, any, any, *grpc.ClientConn, ...grpc.CallOption) error {
		return want
	}

	err = UnaryClientInterceptor(r)(
		t.Context(), listNamespacesMethod,
		&workflowservice.ListNamespacesRequest{}, &workflowservice.ListNamespacesResponse{}, nil, invoker,
	)
	require.Equal(t, want, err, "the caller sees the upstream's status, untranslated")
}

func TestUnaryClientInterceptorReportsConversionFailureAsInternal(t *testing.T) {
	t.Parallel()

	// A reply of the wrong type for the registered translation: the request
	// converts, the call succeeds, and folding the reply back fails.
	r, err := Default()
	require.NoError(t, err)

	invoker := func(context.Context, string, any, any, *grpc.ClientConn, ...grpc.CallOption) error {
		return nil
	}

	err = UnaryClientInterceptor(r)(
		t.Context(), listNamespacesMethod,
		&workflowservice.ListNamespacesRequest{}, &workflowservice.DescribeNamespaceResponse{}, nil, invoker,
	)
	require.Equal(t, codes.Internal, status.Code(err))
}

func TestUnaryClientInterceptorForwardsNonProtoCallsUnchanged(t *testing.T) {
	t.Parallel()

	r, err := Default()
	require.NoError(t, err)

	var gotMethod string
	invoker := func(_ context.Context, method string, _, _ any, _ *grpc.ClientConn, _ ...grpc.CallOption) error {
		gotMethod = method
		return nil
	}

	err = UnaryClientInterceptor(r)(t.Context(), listNamespacesMethod, "req", "reply", nil, invoker)
	require.NoError(t, err)
	require.Equal(t, listNamespacesMethod, gotMethod, "nothing to convert, so nothing is substituted")
}

func TestDialOptionsTranslateOverARealConnection(t *testing.T) {
	t.Parallel()

	upstream := &namespaceLister{
		page: &cloudservice.GetNamespacesResponse{
			Namespaces:    []*cloudnamespace.Namespace{activeNamespace("payments.a1b2c")},
			NextPageToken: "next",
		},
	}

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	svr := grpc.NewServer()
	cloudservice.RegisterCloudServiceServer(svr, upstream)
	go func() { _ = svr.Serve(lis) }()
	t.Cleanup(svr.Stop)

	r, err := Default()
	require.NoError(t, err)

	dialOpts := append(
		[]grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())},
		DialOptions(r)...,
	)
	cc, err := grpc.NewClient(lis.Addr().String(), dialOpts...)
	require.NoError(t, err)
	t.Cleanup(func() { _ = cc.Close() })

	// Invoke the WorkflowService method by name, the way the reflective forwarder
	// does: the interceptor is what turns it into a CloudService call.
	reply := &workflowservice.ListNamespacesResponse{}
	err = cc.Invoke(
		t.Context(), listNamespacesMethod,
		&workflowservice.ListNamespacesRequest{PageSize: 3},
		reply,
	)
	require.NoError(t, err)

	// Cloud rejects GetNamespaces outright without the API version, so the header
	// has to survive the substitution and reach the wire.
	require.Equal(t,
		[]string{cloudclient.DefaultAPIVersion()},
		upstream.gotMD.Get(cloudclient.TemporalCloudAPIVersionHeader()),
	)

	require.Equal(t, int32(3), upstream.got.GetPageSize())
	require.Len(t, reply.GetNamespaces(), 1)
	require.Equal(t, "payments.a1b2c", reply.GetNamespaces()[0].GetNamespaceInfo().GetName())
	require.Equal(t, []byte("next"), reply.GetNextPageToken())
}

func (l *namespaceLister) GetNamespaces(
	ctx context.Context, req *cloudservice.GetNamespacesRequest,
) (*cloudservice.GetNamespacesResponse, error) {
	l.got = req
	l.gotMD, _ = metadata.FromIncomingContext(ctx)

	return l.page, nil
}

func activeNamespace(name string) *cloudnamespace.Namespace {
	return &cloudnamespace.Namespace{
		Namespace: name,
		State:     resource.ResourceState_RESOURCE_STATE_ACTIVE,
		Spec:      &cloudnamespace.NamespaceSpec{Name: name, RetentionDays: 3},
	}
}

func TestUnaryClientInterceptorLeavesUntranslatedCallHeadersAlone(t *testing.T) {
	t.Parallel()

	r, err := Default()
	require.NoError(t, err)

	var gotMD metadata.MD
	invoker := func(ctx context.Context, _ string, _, _ any, _ *grpc.ClientConn, _ ...grpc.CallOption) error {
		gotMD, _ = metadata.FromOutgoingContext(ctx)
		return nil
	}

	// A client calling CloudService directly is forwarded untranslated, so its own
	// API version travels rather than the one the mapping pins.
	ctx := metadata.NewOutgoingContext(
		t.Context(),
		metadata.Pairs(cloudclient.TemporalCloudAPIVersionHeader(), "v0.1.0"),
	)

	err = UnaryClientInterceptor(r)(
		ctx, getNamespacesMethod,
		&cloudservice.GetNamespacesRequest{}, &cloudservice.GetNamespacesResponse{}, nil, invoker,
	)
	require.NoError(t, err)
	require.Equal(t, []string{"v0.1.0"}, gotMD.Get(cloudclient.TemporalCloudAPIVersionHeader()))
}
