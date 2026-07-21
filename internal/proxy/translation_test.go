package proxy

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/api/errordetails/v1"
	namespacepb "go.temporal.io/api/namespace/v1"
	workflowservice "go.temporal.io/api/workflowservice/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoregistry"

	"github.com/temporalio/temporal-proxy/internal/protoutil"
	"github.com/temporalio/temporal-proxy/internal/transport/socket"
)

type (
	// namespaceCheckingService is a fake WorkflowService upstream that rejects
	// any request whose namespace is not the expected translated value.
	namespaceCheckingService struct {
		workflowservice.UnimplementedWorkflowServiceServer
		want string
	}

	// fakeClientStream is a ClientStream whose RecvMsg yields a fixed message.
	fakeClientStream struct {
		grpc.ClientStream
		recv proto.Message
	}
)

func TestUnaryClientInterceptorTranslatesBothDirections(t *testing.T) {
	t.Parallel()

	tr := protoutil.NewTranslator(protoregistry.GlobalFiles)
	interceptor := unaryClientInterceptor(tr, remote, local)

	req := &workflowservice.StartWorkflowExecutionRequest{Namespace: "ns"}
	reply := &workflowservice.DescribeNamespaceResponse{}

	invoker := func(_ context.Context, _ string, gotReq, gotReply any, _ *grpc.ClientConn, _ ...grpc.CallOption) error {
		// The request was translated local to remote before invocation.
		require.Equal(t, "remote-ns", gotReq.(*workflowservice.StartWorkflowExecutionRequest).Namespace)
		// Simulate the upstream returning a remote namespace name in the reply.
		gotReply.(*workflowservice.DescribeNamespaceResponse).NamespaceInfo = namespaceInfo("remote-back")
		return nil
	}

	err := interceptor(t.Context(), "/svc/Method", req, reply, nil, invoker)
	require.NoError(t, err)
	require.Equal(t, "local-remote-back", reply.NamespaceInfo.Name)
}

func TestStreamClientInterceptorTranslatesSendAndRecv(t *testing.T) {
	t.Parallel()

	tr := protoutil.NewTranslator(protoregistry.GlobalFiles)
	fake := &fakeClientStream{}
	streamer := func(context.Context, *grpc.StreamDesc, *grpc.ClientConn, string, ...grpc.CallOption) (grpc.ClientStream, error) {
		return fake, nil
	}

	cs, err := streamClientInterceptor(tr, remote, local)(
		t.Context(), &grpc.StreamDesc{}, nil, "/svc/Stream", streamer,
	)
	require.NoError(t, err)

	sent := &workflowservice.StartWorkflowExecutionRequest{Namespace: "ns"}
	require.NoError(t, cs.SendMsg(sent))
	require.Equal(t, "remote-ns", sent.Namespace)

	fake.recv = &workflowservice.DescribeNamespaceResponse{NamespaceInfo: namespaceInfo("remote-back")}
	got := &workflowservice.DescribeNamespaceResponse{}
	require.NoError(t, cs.RecvMsg(got))
	require.Equal(t, "local-remote-back", got.NamespaceInfo.Name)
}

func TestTranslateStatusErrorRewritesDetailNamespace(t *testing.T) {
	t.Parallel()

	st, err := status.New(codes.NotFound, "namespace not found").
		WithDetails(&errordetails.NamespaceNotFoundFailure{Namespace: "remote-ns"})
	require.NoError(t, err)

	tr := protoutil.NewTranslator(protoregistry.GlobalFiles)
	out := translateStatusError(tr, st.Err(), local)

	outSt, ok := status.FromError(out)
	require.True(t, ok)
	require.Equal(t, "namespace not found", outSt.Message(), "free-text message is unchanged")
	details := outSt.Details()
	require.Len(t, details, 1)
	failure, ok := details[0].(*errordetails.NamespaceNotFoundFailure)
	require.True(t, ok)
	require.Equal(t, "local-remote-ns", failure.Namespace)
}

func TestTranslateStatusErrorPassthroughForNonStatus(t *testing.T) {
	t.Parallel()

	tr := protoutil.NewTranslator(protoregistry.GlobalFiles)
	plain := errors.New("boom")
	require.Equal(t, plain, translateStatusError(tr, plain, local))
}

func TestTranslateStatusErrorNilIsNil(t *testing.T) {
	t.Parallel()

	tr := protoutil.NewTranslator(protoregistry.GlobalFiles)
	require.NoError(t, translateStatusError(tr, nil, local))
}

func TestOutboundNamespaceTranslation(t *testing.T) {
	t.Parallel()

	// Fake upstream frontend on a TCP port. It rejects any request whose
	// namespace is not the translated "remote-local", so a successful call is
	// itself proof that translation happened, with no shared state read from the
	// test goroutine.
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	upstream := grpc.NewServer()
	workflowservice.RegisterWorkflowServiceServer(upstream, &namespaceCheckingService{want: "remote-local"})
	go func() { _ = upstream.Serve(lis) }()
	t.Cleanup(upstream.Stop)

	// Proxy dials the fake upstream, wrapping "local" -> "remote-local".
	translator := protoutil.NewTranslator(protoregistry.GlobalFiles)
	svr, err := New(
		lis.Addr().String(),
		WithDialOptions(translationDialOptions(
			translator,
			func(s string) string { return "remote-" + s },
			func(s string) string { return "local-" + s },
		)...),
	)
	require.NoError(t, err)

	ctx := t.Context()
	go func() { _ = svr.Start(ctx) }()
	t.Cleanup(func() { _ = svr.Stop(context.Background()) })

	conn := dialUnixSocket(t, lis.Addr().String())
	defer func() { _ = conn.Close() }()

	client := workflowservice.NewWorkflowServiceClient(conn)

	require.Eventually(t, func() bool {
		_, err := client.StartWorkflowExecution(ctx, &workflowservice.StartWorkflowExecutionRequest{Namespace: "local"})
		return err == nil
	}, 3*time.Second, 20*time.Millisecond, "upstream should observe the translated namespace")
}

func (s *namespaceCheckingService) StartWorkflowExecution(
	_ context.Context, req *workflowservice.StartWorkflowExecutionRequest,
) (*workflowservice.StartWorkflowExecutionResponse, error) {
	if req.Namespace != s.want {
		return nil, status.Errorf(codes.InvalidArgument, "namespace = %q, want %q", req.Namespace, s.want)
	}

	return &workflowservice.StartWorkflowExecutionResponse{}, nil
}

func (f *fakeClientStream) SendMsg(any) error { return nil }

func (f *fakeClientStream) RecvMsg(m any) error {
	proto.Merge(m.(proto.Message), f.recv)
	return nil
}

func remote(s string) string { return "remote-" + s }

func local(s string) string { return "local-" + s }

func namespaceInfo(name string) *namespacepb.NamespaceInfo {
	return &namespacepb.NamespaceInfo{Name: name}
}

// dialUnixSocket returns a client connection to the proxy's unix socket for the
// given upstream host. The socket path matches what proxy.Start binds.
func dialUnixSocket(t *testing.T, upstream string) *grpc.ClientConn {
	t.Helper()

	path, err := socket.UnixPath(upstream)
	require.NoError(t, err)

	conn, err := grpc.NewClient(
		"unix://"+path,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)
	return conn
}
