package proxy

// forwarder_mocks_test.go is generated into package proxy (not proxy_test) because
// forwardBidiStream is unexported and can only be tested from within the package.
// The stream mocks in admin_mocks_test.go live in proxy_test and are not accessible here.
//go:generate go tool -modfile=../../dev/tools.mod mockgen -destination forwarder_mocks_test.go -package proxy -typed go.temporal.io/server/api/adminservice/v1 AdminService_StreamWorkflowReplicationMessagesClient,AdminService_StreamWorkflowReplicationMessagesServer

import (
	"context"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
	workflowservicev1 "go.temporal.io/api/workflowservice/v1"
	adminservicev1 "go.temporal.io/server/api/adminservice/v1"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func TestForwardUnary_CopiesIncomingMetadata(t *testing.T) {
	t.Parallel()

	ctx := metadata.NewIncomingContext(
		context.Background(),
		metadata.Pairs("x-custom-header", "test-value"),
	)

	var capturedCtx context.Context
	_, err := forwardUnary(ctx, &workflowservicev1.GetSystemInfoRequest{},
		func(ctx context.Context, _ *workflowservicev1.GetSystemInfoRequest, _ ...grpc.CallOption) (*workflowservicev1.GetSystemInfoResponse, error) {
			capturedCtx = ctx
			return &workflowservicev1.GetSystemInfoResponse{}, nil
		},
	)
	require.NoError(t, err)

	md, ok := metadata.FromOutgoingContext(capturedCtx)
	require.True(t, ok)
	require.Equal(t, []string{"test-value"}, md.Get("x-custom-header"))
}

func TestForwardUnary_PropagatesUpstreamError(t *testing.T) {
	t.Parallel()

	expected := status.Error(codes.NotFound, "namespace not found")
	_, err := forwardUnary(context.Background(), &workflowservicev1.GetSystemInfoRequest{},
		func(_ context.Context, _ *workflowservicev1.GetSystemInfoRequest, _ ...grpc.CallOption) (*workflowservicev1.GetSystemInfoResponse, error) {
			return nil, expected
		},
	)

	require.Error(t, err)
	require.Equal(t, codes.NotFound, status.Code(err))
}

func TestForwardUnary_NoIncomingMetadata(t *testing.T) {
	t.Parallel()

	// Should not panic when there is no incoming metadata.
	result, err := forwardUnary(context.Background(), &workflowservicev1.GetSystemInfoRequest{},
		func(_ context.Context, _ *workflowservicev1.GetSystemInfoRequest, _ ...grpc.CallOption) (*workflowservicev1.GetSystemInfoResponse, error) {
			return &workflowservicev1.GetSystemInfoResponse{}, nil
		},
	)

	require.NoError(t, err)
	require.NotNil(t, result)
}

func TestForwardBidiStream(t *testing.T) {
	t.Parallel()

	type openStreamFn func(context.Context, ...grpc.CallOption) (adminservicev1.AdminService_StreamWorkflowReplicationMessagesClient, error)
	type setupFn func(*testing.T, *gomock.Controller, *MockAdminService_StreamWorkflowReplicationMessagesServer) openStreamFn

	tests := []struct {
		name    string
		setup   setupFn
		wantErr codes.Code
	}{
		{
			name: "upstream open error",
			setup: func(_ *testing.T, _ *gomock.Controller, server *MockAdminService_StreamWorkflowReplicationMessagesServer) openStreamFn {
				server.EXPECT().Context().Return(context.Background()).AnyTimes()
				return func(_ context.Context, _ ...grpc.CallOption) (adminservicev1.AdminService_StreamWorkflowReplicationMessagesClient, error) {
					return nil, status.Error(codes.Unavailable, "no upstream")
				}
			},
			wantErr: codes.Internal,
		},
		{
			name: "pumps messages both directions",
			setup: func(_ *testing.T, ctrl *gomock.Controller, server *MockAdminService_StreamWorkflowReplicationMessagesServer) openStreamFn {
				msg := &adminservicev1.StreamWorkflowReplicationMessagesRequest{}
				resp := &adminservicev1.StreamWorkflowReplicationMessagesResponse{}
				cstream := NewMockAdminService_StreamWorkflowReplicationMessagesClient(ctrl)

				server.EXPECT().Context().Return(context.Background()).AnyTimes()
				server.EXPECT().Recv().Return(msg, nil).Times(1)
				server.EXPECT().Recv().Return(nil, io.EOF)
				server.EXPECT().Send(resp).Return(nil)

				cstream.EXPECT().Send(msg).Return(nil)
				cstream.EXPECT().CloseSend().Return(nil)
				cstream.EXPECT().Recv().Return(resp, nil).Times(1)
				cstream.EXPECT().Recv().Return(nil, io.EOF)

				return func(_ context.Context, _ ...grpc.CallOption) (adminservicev1.AdminService_StreamWorkflowReplicationMessagesClient, error) {
					return cstream, nil
				}
			},
		},
		{
			name: "server recv error propagates",
			setup: func(_ *testing.T, ctrl *gomock.Controller, server *MockAdminService_StreamWorkflowReplicationMessagesServer) openStreamFn {
				cstream := NewMockAdminService_StreamWorkflowReplicationMessagesClient(ctrl)

				server.EXPECT().Context().Return(context.Background()).AnyTimes()
				server.EXPECT().Recv().Return(nil, status.Error(codes.Internal, "server recv error"))
				cstream.EXPECT().CloseSend().Return(nil)
				// upstream→server goroutine runs concurrently; allow 0+ Recv calls
				cstream.EXPECT().Recv().Return(nil, io.EOF).AnyTimes()

				return func(_ context.Context, _ ...grpc.CallOption) (adminservicev1.AdminService_StreamWorkflowReplicationMessagesClient, error) {
					return cstream, nil
				}
			},
			wantErr: codes.Internal,
		},
		{
			// This is the previously-uncovered path: clientStream.Send fails without
			// calling CloseSend (see forwarder.go lines 49-51). Verified by the
			// absence of a CloseSend expectation here.
			name: "client send error propagates",
			setup: func(_ *testing.T, ctrl *gomock.Controller, server *MockAdminService_StreamWorkflowReplicationMessagesServer) openStreamFn {
				msg := &adminservicev1.StreamWorkflowReplicationMessagesRequest{}
				cstream := NewMockAdminService_StreamWorkflowReplicationMessagesClient(ctrl)

				server.EXPECT().Context().Return(context.Background()).AnyTimes()
				server.EXPECT().Recv().Return(msg, nil).Times(1)
				cstream.EXPECT().Send(msg).Return(status.Error(codes.Unavailable, "client send failed"))
				cstream.EXPECT().Recv().Return(nil, io.EOF).AnyTimes()

				return func(_ context.Context, _ ...grpc.CallOption) (adminservicev1.AdminService_StreamWorkflowReplicationMessagesClient, error) {
					return cstream, nil
				}
			},
			wantErr: codes.Unavailable,
		},
		{
			name: "client recv error propagates",
			setup: func(_ *testing.T, ctrl *gomock.Controller, server *MockAdminService_StreamWorkflowReplicationMessagesServer) openStreamFn {
				cstream := NewMockAdminService_StreamWorkflowReplicationMessagesClient(ctrl)

				server.EXPECT().Context().Return(context.Background()).AnyTimes()
				server.EXPECT().Recv().Return(nil, io.EOF)
				cstream.EXPECT().CloseSend().Return(nil)
				cstream.EXPECT().Recv().Return(nil, status.Error(codes.Unavailable, "client recv error"))

				return func(_ context.Context, _ ...grpc.CallOption) (adminservicev1.AdminService_StreamWorkflowReplicationMessagesClient, error) {
					return cstream, nil
				}
			},
			wantErr: codes.Unavailable,
		},
		{
			name: "server send error propagates",
			setup: func(_ *testing.T, ctrl *gomock.Controller, server *MockAdminService_StreamWorkflowReplicationMessagesServer) openStreamFn {
				resp := &adminservicev1.StreamWorkflowReplicationMessagesResponse{}
				cstream := NewMockAdminService_StreamWorkflowReplicationMessagesClient(ctrl)

				server.EXPECT().Context().Return(context.Background()).AnyTimes()
				server.EXPECT().Send(resp).Return(status.Error(codes.ResourceExhausted, "server send error"))
				server.EXPECT().Recv().Return(nil, io.EOF).AnyTimes()

				cstream.EXPECT().Recv().Return(resp, nil)
				cstream.EXPECT().CloseSend().Return(nil).Times(1)
				cstream.EXPECT().Recv().Return(nil, io.EOF).AnyTimes()

				return func(_ context.Context, _ ...grpc.CallOption) (adminservicev1.AdminService_StreamWorkflowReplicationMessagesClient, error) {
					return cstream, nil
				}
			},
			wantErr: codes.ResourceExhausted,
		},
		{
			name: "metadata forwarded to upstream",
			setup: func(t *testing.T, _ *gomock.Controller, server *MockAdminService_StreamWorkflowReplicationMessagesServer) openStreamFn {
				ctx := metadata.NewIncomingContext(
					context.Background(),
					metadata.Pairs("x-test-key", "test-value"),
				)

				server.EXPECT().Context().Return(ctx).AnyTimes()
				return func(ctx context.Context, _ ...grpc.CallOption) (adminservicev1.AdminService_StreamWorkflowReplicationMessagesClient, error) {
					md, ok := metadata.FromOutgoingContext(ctx)
					require.True(t, ok)
					require.Equal(t, []string{"test-value"}, md.Get("x-test-key"))
					return nil, status.Error(codes.Unavailable, "done")
				}
			},
			wantErr: codes.Internal,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctrl := gomock.NewController(t)
			mockServer := NewMockAdminService_StreamWorkflowReplicationMessagesServer(ctrl)
			openStream := tt.setup(t, ctrl, mockServer)

			err := forwardBidiStream(mockServer, openStream)

			if tt.wantErr != codes.OK {
				require.Error(t, err)
				require.Equal(t, tt.wantErr, status.Code(err))
			} else {
				require.NoError(t, err)
			}
		})
	}
}
