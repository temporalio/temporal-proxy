package proxy_test

import (
	"net"
	"testing"

	"github.com/stretchr/testify/require"
	workflowservice "go.temporal.io/api/workflowservice/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	"github.com/temporalio/temporal-proxy/internal/proxy"
	"github.com/temporalio/temporal-proxy/internal/transport/meta"
)

func TestVersionDialOptionsUnary(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		version string
		ctx     func(*testing.T) metadata.MD
		want    []string
	}{
		{
			name:    "the version reaches the upstream",
			version: "1.4.2",
			want:    []string{"1.4.2"},
		},
		{
			// The forwarder relays the caller's metadata onward, so a client sending
			// the header itself must not arrive alongside the real version.
			name:    "a client-supplied value is replaced",
			version: "1.4.2",
			ctx:     func(*testing.T) metadata.MD { return metadata.Pairs(meta.VersionHeader, "spoofed") },
			want:    []string{"1.4.2"},
		},
		{
			name:    "an empty version stamps nothing",
			version: "",
			want:    nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			addr, got := capturingServer(t)
			cc := clientWithOptions(t, addr, proxy.VersionDialOptions(tt.version))

			ctx := t.Context()
			if tt.ctx != nil {
				ctx = metadata.NewOutgoingContext(ctx, tt.ctx(t))
			}

			err := cc.Invoke(
				ctx,
				"/svc/Method",
				&workflowservice.StartWorkflowExecutionRequest{},
				&workflowservice.StartWorkflowExecutionResponse{},
			)
			require.NoError(t, err)

			require.Equal(t, tt.want, (<-got).Get(meta.VersionHeader))
		})
	}
}

func TestVersionDialOptionsStream(t *testing.T) {
	t.Parallel()

	addr, got := capturingServer(t)
	cc := clientWithOptions(t, addr, proxy.VersionDialOptions("1.4.2"))

	cs, err := cc.NewStream(
		t.Context(),
		&grpc.StreamDesc{ClientStreams: true, ServerStreams: true},
		"/svc/Stream",
	)
	require.NoError(t, err)

	require.NoError(t, cs.SendMsg(&workflowservice.StartWorkflowExecutionRequest{}))

	require.Equal(t, []string{"1.4.2"}, (<-got).Get(meta.VersionHeader))
}

func TestVersionDialOptionsLeavesOtherMetadataAlone(t *testing.T) {
	t.Parallel()

	addr, got := capturingServer(t)
	cc := clientWithOptions(t, addr, proxy.VersionDialOptions("1.4.2"))

	err := cc.Invoke(
		meta.WithNamespace(t.Context(), "orders"),
		"/svc/Method",
		&workflowservice.StartWorkflowExecutionRequest{},
		&workflowservice.StartWorkflowExecutionResponse{},
	)
	require.NoError(t, err)

	md := <-got
	require.Equal(t, []string{"orders"}, md.Get(meta.NamespaceHeader))
	require.Equal(t, []string{"1.4.2"}, md.Get(meta.VersionHeader))
}

// capturingServer starts a gRPC server that accepts any method, records the
// metadata each stream opened with, and answers every message with an empty
// response so a unary call completes. It returns its address and the channel the
// metadata arrives on.
func capturingServer(t *testing.T) (string, <-chan metadata.MD) {
	t.Helper()

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	got := make(chan metadata.MD, 1)
	srv := grpc.NewServer(grpc.UnknownServiceHandler(func(_ any, stream grpc.ServerStream) error {
		md, _ := metadata.FromIncomingContext(stream.Context())
		select {
		case got <- md:
		default:
		}

		for {
			if err := stream.RecvMsg(new(workflowservice.StartWorkflowExecutionRequest)); err != nil {
				return nil
			}

			if err := stream.SendMsg(&workflowservice.StartWorkflowExecutionResponse{}); err != nil {
				return nil
			}
		}
	}))

	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)

	return lis.Addr().String(), got
}
