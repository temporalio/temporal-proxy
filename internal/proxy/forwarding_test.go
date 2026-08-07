package proxy_test

// These tests drive real requests through a real [proxy.Server] over its unix
// socket, so they use only the exported surface and live in the external test
// package. forward_test.go stays in package proxy for the unit tests that need
// the forwarder's unexported internals.

import (
	"context"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
	workflowservice "go.temporal.io/api/workflowservice/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/reflection/grpc_reflection_v1"
	"google.golang.org/grpc/status"

	"github.com/temporalio/temporal-proxy/internal/services"
)

// metadataStampingService is a fake WorkflowService upstream that sets a response
// header and trailer, so a caller can tell whether the proxy relayed them.
type metadataStampingService struct {
	workflowservice.UnimplementedWorkflowServiceServer
}

func TestUnaryRelaysUpstreamHeaderAndTrailer(t *testing.T) {
	t.Parallel()

	// Invoke drops the upstream's response metadata unless asked for it, and the
	// loss is invisible to a caller that does not read it, so this drives a real
	// request through a real proxy and asserts both arrive.
	addr := serveUpstream(t, func(s *grpc.Server) {
		workflowservice.RegisterWorkflowServiceServer(s, &metadataStampingService{})
	})
	conn := startProxy(t, addr)

	var header, trailer metadata.MD
	_, err := workflowservice.NewWorkflowServiceClient(conn).GetSystemInfo(
		t.Context(),
		&workflowservice.GetSystemInfoRequest{},
		grpc.Header(&header),
		grpc.Trailer(&trailer),
		grpc.WaitForReady(true),
	)

	require.NoError(t, err)
	require.Equal(t, []string{"hdr"}, header.Get("x-upstream-header"))
	require.Equal(t, []string{"tlr"}, trailer.Get("x-upstream-trailer"))
}

func TestStreamForwardsBidiReflection(t *testing.T) {
	t.Parallel()

	// ServerReflectionInfo is the only streaming method across every forwardable
	// service, so it is the only way to exercise the streaming path at all. The
	// upstream is the sole reflection provider (the proxy's local server registers
	// only the health service), so a response naming WorkflowService proves the
	// stream reached it and came back.
	addr := serveUpstream(t, func(s *grpc.Server) {
		workflowservice.RegisterWorkflowServiceServer(s, &metadataStampingService{})
		reflection.Register(s)
	})
	conn := startProxy(t, addr, services.Reflection)

	stream, err := grpc_reflection_v1.NewServerReflectionClient(conn).ServerReflectionInfo(
		t.Context(),
		grpc.WaitForReady(true),
	)
	require.NoError(t, err)
	require.NoError(t, stream.Send(&grpc_reflection_v1.ServerReflectionRequest{
		MessageRequest: &grpc_reflection_v1.ServerReflectionRequest_ListServices{},
	}))

	resp, err := stream.Recv()
	require.NoError(t, err)

	var got []string
	for _, svc := range resp.GetListServicesResponse().GetService() {
		got = append(got, svc.GetName())
	}

	require.Contains(t, got, services.WorkflowService, "expected the upstream's services, not the proxy's")

	// Half-close and drain, so the request pump reports a clean end of stream and
	// the response pump completes rather than the test tearing the stream down.
	require.NoError(t, stream.CloseSend())

	_, err = stream.Recv()
	require.ErrorIs(t, err, io.EOF)
}

func TestUnaryWithoutRequestMessageFails(t *testing.T) {
	t.Parallel()

	// A caller that half-closes without sending a request makes RecvMsg report
	// io.EOF, which carries no gRPC status. Left unmapped it reaches the caller as
	// an opaque Unknown, so this pins the mapping.
	addr := serveUpstream(t, func(s *grpc.Server) {
		workflowservice.RegisterWorkflowServiceServer(s, &metadataStampingService{})
	})
	conn := startProxy(t, addr)

	// Driving a unary method as a raw stream is the only way to reach the proxy
	// without sending a message; the generated client always sends one.
	stream, err := conn.NewStream(
		t.Context(),
		&grpc.StreamDesc{ServerStreams: true, ClientStreams: true},
		"/"+services.WorkflowService+"/GetSystemInfo",
		grpc.WaitForReady(true),
	)
	require.NoError(t, err)
	require.NoError(t, stream.CloseSend())

	err = stream.RecvMsg(&workflowservice.GetSystemInfoResponse{})
	require.Equal(t, codes.Internal, status.Code(err))
	require.ErrorContains(t, err, "reading the request failed",
		"expected the status to name the step that failed, not the request stream generally")
}

func (*metadataStampingService) GetSystemInfo(
	ctx context.Context, _ *workflowservice.GetSystemInfoRequest,
) (*workflowservice.GetSystemInfoResponse, error) {
	if err := grpc.SetHeader(ctx, metadata.Pairs("x-upstream-header", "hdr")); err != nil {
		return nil, err
	}

	if err := grpc.SetTrailer(ctx, metadata.Pairs("x-upstream-trailer", "tlr")); err != nil {
		return nil, err
	}

	return &workflowservice.GetSystemInfoResponse{}, nil
}
