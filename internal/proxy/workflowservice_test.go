package proxy_test

//go:generate go tool -modfile=../../dev/tools.mod mockgen -destination workflow_mocks_test.go -package proxy_test -typed go.temporal.io/api/workflowservice/v1 WorkflowServiceClient

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	workflowservicev1 "go.temporal.io/api/workflowservice/v1"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/temporalio/temporal-proxy/internal/proxy"
)

func TestWorkflowServiceProxy(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		setup   func(*MockWorkflowServiceClient)
		call    func(workflowservicev1.WorkflowServiceServer) error
		wantErr codes.Code
	}{
		{
			name: "forwards request and response",
			setup: func(m *MockWorkflowServiceClient) {
				req := &workflowservicev1.StartWorkflowExecutionRequest{Namespace: "test-ns"}
				m.EXPECT().
					StartWorkflowExecution(gomock.Any(), req, gomock.Any()).
					Return(&workflowservicev1.StartWorkflowExecutionResponse{RunId: "run-1"}, nil)
			},
			call: func(svc workflowservicev1.WorkflowServiceServer) error {
				resp, err := svc.StartWorkflowExecution(t.Context(), &workflowservicev1.StartWorkflowExecutionRequest{Namespace: "test-ns"})
				if err != nil {
					return err
				}
				if resp.GetRunId() != "run-1" {
					return status.Error(codes.Internal, "unexpected RunId")
				}
				return nil
			},
		},
		{
			name: "propagates upstream error code",
			setup: func(m *MockWorkflowServiceClient) {
				m.EXPECT().
					GetSystemInfo(gomock.Any(), gomock.Any(), gomock.Any()).
					Return(nil, status.Error(codes.Unavailable, "no upstream"))
			},
			call: func(svc workflowservicev1.WorkflowServiceServer) error {
				_, err := svc.GetSystemInfo(t.Context(), &workflowservicev1.GetSystemInfoRequest{})
				return err
			},
			wantErr: codes.Unavailable,
		},
		{
			name: "SignalWorkflowExecution forwards request and response",
			setup: func(m *MockWorkflowServiceClient) {
				req := &workflowservicev1.SignalWorkflowExecutionRequest{Namespace: "test-ns"}
				m.EXPECT().
					SignalWorkflowExecution(gomock.Any(), req, gomock.Any()).
					Return(&workflowservicev1.SignalWorkflowExecutionResponse{}, nil)
			},
			call: func(svc workflowservicev1.WorkflowServiceServer) error {
				_, err := svc.SignalWorkflowExecution(t.Context(), &workflowservicev1.SignalWorkflowExecutionRequest{Namespace: "test-ns"})
				return err
			},
		},
		{
			name: "QueryWorkflow forwards request and response",
			setup: func(m *MockWorkflowServiceClient) {
				m.EXPECT().
					QueryWorkflow(gomock.Any(), gomock.Any(), gomock.Any()).
					Return(&workflowservicev1.QueryWorkflowResponse{}, nil)
			},
			call: func(svc workflowservicev1.WorkflowServiceServer) error {
				_, err := svc.QueryWorkflow(t.Context(), &workflowservicev1.QueryWorkflowRequest{})
				return err
			},
		},
		{
			name: "PollWorkflowTaskQueue propagates upstream error",
			setup: func(m *MockWorkflowServiceClient) {
				m.EXPECT().
					PollWorkflowTaskQueue(gomock.Any(), gomock.Any(), gomock.Any()).
					Return(nil, status.Error(codes.Unavailable, "no upstream"))
			},
			call: func(svc workflowservicev1.WorkflowServiceServer) error {
				_, err := svc.PollWorkflowTaskQueue(t.Context(), &workflowservicev1.PollWorkflowTaskQueueRequest{})
				return err
			},
			wantErr: codes.Unavailable,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctrl := gomock.NewController(t)
			client := NewMockWorkflowServiceClient(ctrl)
			tt.setup(client)

			svc := proxy.NewWorkflowServiceProxy(client)
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

func TestWorkflowServiceProxy_CopiesMetadata(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	client := NewMockWorkflowServiceClient(ctrl)
	svc := proxy.NewWorkflowServiceProxy(client)

	ctx := metadata.NewIncomingContext(
		t.Context(),
		metadata.Pairs("x-request-id", "abc-123"),
	)

	client.EXPECT().
		DescribeNamespace(gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, _ *workflowservicev1.DescribeNamespaceRequest, _ ...grpc.CallOption) (*workflowservicev1.DescribeNamespaceResponse, error) {
			md, ok := metadata.FromOutgoingContext(ctx)
			require.True(t, ok)
			require.Equal(t, []string{"abc-123"}, md.Get("x-request-id"))
			return &workflowservicev1.DescribeNamespaceResponse{}, nil
		})

	_, err := svc.DescribeNamespace(ctx, &workflowservicev1.DescribeNamespaceRequest{})
	require.NoError(t, err)
}
