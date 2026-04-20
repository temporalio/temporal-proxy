package proxy_test

//go:generate go tool -modfile=../../dev/tools.mod mockgen -destination admin_mocks_test.go -package proxy_test -typed go.temporal.io/server/api/adminservice/v1 AdminServiceClient,AdminService_StreamWorkflowReplicationMessagesClient,AdminService_StreamWorkflowReplicationMessagesServer

import (
	"context"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
	adminservicev1 "go.temporal.io/server/api/adminservice/v1"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/temporalio/temporal-proxy/internal/proxy"
)

func TestAdminServiceProxy(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		setup   func(*MockAdminServiceClient)
		call    func(adminservicev1.AdminServiceServer) error
		wantErr codes.Code
	}{
		{
			name: "forwards request and response",
			setup: func(m *MockAdminServiceClient) {
				req := &adminservicev1.DescribeClusterRequest{}
				m.EXPECT().
					DescribeCluster(gomock.Any(), req, gomock.Any()).
					Return(&adminservicev1.DescribeClusterResponse{ClusterId: "cluster-1"}, nil)
			},
			call: func(svc adminservicev1.AdminServiceServer) error {
				resp, err := svc.DescribeCluster(t.Context(), &adminservicev1.DescribeClusterRequest{})
				if err != nil {
					return err
				}
				if resp.GetClusterId() != "cluster-1" {
					return status.Error(codes.Internal, "unexpected ClusterId")
				}
				return nil
			},
		},
		{
			name: "propagates upstream error code",
			setup: func(m *MockAdminServiceClient) {
				m.EXPECT().
					DescribeCluster(gomock.Any(), gomock.Any(), gomock.Any()).
					Return(nil, status.Error(codes.Internal, "upstream error"))
			},
			call: func(svc adminservicev1.AdminServiceServer) error {
				_, err := svc.DescribeCluster(t.Context(), &adminservicev1.DescribeClusterRequest{})
				return err
			},
			wantErr: codes.Internal,
		},
		{
			name: "AddOrUpdateRemoteCluster forwards request and response",
			setup: func(m *MockAdminServiceClient) {
				req := &adminservicev1.AddOrUpdateRemoteClusterRequest{FrontendAddress: "remote:7233"}
				m.EXPECT().
					AddOrUpdateRemoteCluster(gomock.Any(), req, gomock.Any()).
					Return(&adminservicev1.AddOrUpdateRemoteClusterResponse{}, nil)
			},
			call: func(svc adminservicev1.AdminServiceServer) error {
				_, err := svc.AddOrUpdateRemoteCluster(t.Context(), &adminservicev1.AddOrUpdateRemoteClusterRequest{FrontendAddress: "remote:7233"})
				return err
			},
		},
		{
			name: "GetNamespace forwards request and response",
			setup: func(m *MockAdminServiceClient) {
				m.EXPECT().
					GetNamespace(gomock.Any(), gomock.Any(), gomock.Any()).
					Return(&adminservicev1.GetNamespaceResponse{}, nil)
			},
			call: func(svc adminservicev1.AdminServiceServer) error {
				_, err := svc.GetNamespace(t.Context(), &adminservicev1.GetNamespaceRequest{})
				return err
			},
		},
		{
			name: "SyncWorkflowState propagates upstream error",
			setup: func(m *MockAdminServiceClient) {
				m.EXPECT().
					SyncWorkflowState(gomock.Any(), gomock.Any(), gomock.Any()).
					Return(nil, status.Error(codes.NotFound, "workflow not found"))
			},
			call: func(svc adminservicev1.AdminServiceServer) error {
				_, err := svc.SyncWorkflowState(t.Context(), &adminservicev1.SyncWorkflowStateRequest{})
				return err
			},
			wantErr: codes.NotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctrl := gomock.NewController(t)
			mockClient := NewMockAdminServiceClient(ctrl)
			tt.setup(mockClient)

			svc := proxy.NewAdminServiceProxy(mockClient)
			err := tt.call(svc)

			if tt.wantErr != codes.OK {
				require.Error(t, err)
				require.Equal(t, tt.wantErr, status.Code(err))
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestAdminServiceProxy_StreamWorkflowReplicationMessages(t *testing.T) {
	t.Parallel()

	type setupFn func(t *testing.T, ctrl *gomock.Controller, client *MockAdminServiceClient, server *MockAdminService_StreamWorkflowReplicationMessagesServer)

	tests := []struct {
		name    string
		setup   setupFn
		wantErr codes.Code
	}{
		{
			name: "upstream open error",
			setup: func(_ *testing.T, _ *gomock.Controller, client *MockAdminServiceClient, server *MockAdminService_StreamWorkflowReplicationMessagesServer) {
				server.EXPECT().Context().Return(t.Context()).AnyTimes()
				client.EXPECT().
					StreamWorkflowReplicationMessages(gomock.Any(), gomock.Any()).
					Return(nil, status.Error(codes.Unavailable, "no upstream"))
			},
			wantErr: codes.Internal,
		},
		{
			name: "pumps messages both directions",
			setup: func(_ *testing.T, ctrl *gomock.Controller, client *MockAdminServiceClient, server *MockAdminService_StreamWorkflowReplicationMessagesServer) {
				msg := &adminservicev1.StreamWorkflowReplicationMessagesRequest{}
				resp := &adminservicev1.StreamWorkflowReplicationMessagesResponse{}
				clientStream := NewMockAdminService_StreamWorkflowReplicationMessagesClient(ctrl)

				server.EXPECT().Context().Return(t.Context()).AnyTimes()
				client.EXPECT().
					StreamWorkflowReplicationMessages(gomock.Any(), gomock.Any()).
					Return(clientStream, nil)

				// server → upstream: one message then EOF
				server.EXPECT().Recv().Return(msg, nil).Times(1)
				server.EXPECT().Recv().Return(nil, io.EOF)
				clientStream.EXPECT().Send(msg).Return(nil)
				clientStream.EXPECT().CloseSend().Return(nil)

				// upstream → server: one message then EOF
				clientStream.EXPECT().Recv().Return(resp, nil).Times(1)
				server.EXPECT().Send(resp).Return(nil)
				clientStream.EXPECT().Recv().Return(nil, io.EOF)
			},
		},
		{
			name: "server recv error propagates",
			setup: func(_ *testing.T, ctrl *gomock.Controller, client *MockAdminServiceClient, server *MockAdminService_StreamWorkflowReplicationMessagesServer) {
				clientStream := NewMockAdminService_StreamWorkflowReplicationMessagesClient(ctrl)

				server.EXPECT().Context().Return(t.Context()).AnyTimes()
				client.EXPECT().
					StreamWorkflowReplicationMessages(gomock.Any(), gomock.Any()).
					Return(clientStream, nil)

				server.EXPECT().Recv().Return(nil, status.Error(codes.Internal, "server recv error"))
				clientStream.EXPECT().CloseSend().Return(nil)
				// upstream → server goroutine runs concurrently; may call Recv 0+ times
				clientStream.EXPECT().Recv().Return(nil, io.EOF).AnyTimes()
			},
			wantErr: codes.Internal,
		},
		{
			name: "client recv error propagates",
			setup: func(_ *testing.T, ctrl *gomock.Controller, client *MockAdminServiceClient, server *MockAdminService_StreamWorkflowReplicationMessagesServer) {
				clientStream := NewMockAdminService_StreamWorkflowReplicationMessagesClient(ctrl)

				server.EXPECT().Context().Return(t.Context()).AnyTimes()
				client.EXPECT().
					StreamWorkflowReplicationMessages(gomock.Any(), gomock.Any()).
					Return(clientStream, nil)

				// server → upstream: clean EOF
				server.EXPECT().Recv().Return(nil, io.EOF)
				clientStream.EXPECT().CloseSend().Return(nil)

				// upstream → server: error
				clientStream.EXPECT().Recv().Return(nil, status.Error(codes.Unavailable, "client recv error"))
			},
			wantErr: codes.Unavailable,
		},
		{
			name: "server send error propagates",
			setup: func(_ *testing.T, ctrl *gomock.Controller, client *MockAdminServiceClient, server *MockAdminService_StreamWorkflowReplicationMessagesServer) {
				clientStream := NewMockAdminService_StreamWorkflowReplicationMessagesClient(ctrl)
				resp := &adminservicev1.StreamWorkflowReplicationMessagesResponse{}

				server.EXPECT().Context().Return(t.Context()).AnyTimes()
				client.EXPECT().
					StreamWorkflowReplicationMessages(gomock.Any(), gomock.Any()).
					Return(clientStream, nil)

				// upstream → server: receives one response, then Send fails
				clientStream.EXPECT().Recv().Return(resp, nil)
				server.EXPECT().Send(resp).Return(status.Error(codes.ResourceExhausted, "send error"))

				// server → upstream goroutine runs concurrently; may call Recv 0+ times
				server.EXPECT().Recv().Return(nil, io.EOF).AnyTimes()
				clientStream.EXPECT().CloseSend().Return(nil).AnyTimes()
				clientStream.EXPECT().Recv().Return(nil, io.EOF).AnyTimes()
			},
			wantErr: codes.ResourceExhausted,
		},
		{
			name: "incoming metadata forwarded to upstream",
			setup: func(t *testing.T, _ *gomock.Controller, client *MockAdminServiceClient, server *MockAdminService_StreamWorkflowReplicationMessagesServer) {
				incomingCtx := metadata.NewIncomingContext(
					t.Context(),
					metadata.Pairs("x-test-key", "test-value"),
				)
				server.EXPECT().Context().Return(incomingCtx).AnyTimes()
				client.EXPECT().
					StreamWorkflowReplicationMessages(gomock.Any(), gomock.Any()).
					DoAndReturn(func(ctx context.Context, _ ...grpc.CallOption) (adminservicev1.AdminService_StreamWorkflowReplicationMessagesClient, error) {
						md, ok := metadata.FromOutgoingContext(ctx)
						require.True(t, ok)
						require.Equal(t, []string{"test-value"}, md.Get("x-test-key"))
						return nil, status.Error(codes.Unavailable, "done")
					})
			},
			wantErr: codes.Internal, // forwardBidiStream wraps open-stream errors in codes.Internal
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctrl := gomock.NewController(t)
			mockClient := NewMockAdminServiceClient(ctrl)
			mockServerStream := NewMockAdminService_StreamWorkflowReplicationMessagesServer(ctrl)

			tt.setup(t, ctrl, mockClient, mockServerStream)

			svc := proxy.NewAdminServiceProxy(mockClient)
			err := svc.StreamWorkflowReplicationMessages(mockServerStream)

			if tt.wantErr != codes.OK {
				require.Error(t, err)
				require.Equal(t, tt.wantErr, status.Code(err))
			} else {
				require.NoError(t, err)
			}
		})
	}
}
