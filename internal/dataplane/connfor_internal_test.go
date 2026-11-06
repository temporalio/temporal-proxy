package dataplane

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/api/workflowservice/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"go.temporal.io/proxy/internal/services"
	"go.temporal.io/proxy/internal/transport/meta"
)

type (
	// fakeConn hands out a recordingStream and records the context it was
	// opened with.
	fakeConn struct {
		grpc.ClientConnInterface

		stream *recordingStream
		ctx    context.Context
	}

	// recordingStream records every message that reaches it.
	recordingStream struct {
		grpc.ClientStream

		sent []any
	}
)

func (c *fakeConn) NewStream(ctx context.Context, _ *grpc.StreamDesc, _ string, _ ...grpc.CallOption) (grpc.ClientStream, error) {
	c.ctx = ctx
	return c.stream, nil
}

func (s *recordingStream) SendMsg(m any) error {
	s.sent = append(s.sent, m)
	return nil
}

func TestNamespaceStreamChecksEachSentMessage(t *testing.T) {
	t.Parallel()

	fake := &fakeConn{stream: &recordingStream{}}
	conn := &namespaceConn{
		cc:        fake,
		namespace: "orders",
		allowlist: services.NewAllowlist(services.Known()),
	}

	ctx := metadata.AppendToOutgoingContext(t.Context(), meta.TemporalNamespaceHeader, "payments")
	cs, err := conn.NewStream(ctx, &grpc.StreamDesc{ClientStreams: true}, "/"+services.WorkflowService+"/Stream")
	require.NoError(t, err)

	md, _ := metadata.FromOutgoingContext(fake.ctx)
	require.Equal(t, []string{"orders"}, md.Get(meta.NamespaceHeader))
	require.Equal(t, []string{"orders"}, md.Get(meta.TemporalNamespaceHeader))

	ok := &workflowservice.StartNexusOperationExecutionRequest{Namespace: "orders"}
	require.NoError(t, cs.SendMsg(ok))

	for _, m := range []any{
		&workflowservice.StartNexusOperationExecutionRequest{Namespace: "payments"},
		&workflowservice.StartNexusOperationExecutionRequest{},
		&workflowservice.GetSystemInfoRequest{},
	} {
		err := cs.SendMsg(m)
		require.Equal(t, codes.InvalidArgument, status.Code(err), "%T %v", m, m)
	}

	require.Equal(t, []any{ok}, fake.stream.sent, "only the message naming the bound namespace may be sent")
}
