package proxy

import (
	"context"

	adminservicev1 "go.temporal.io/server/api/adminservice/v1"
)

type AdminServiceProxy struct {
	adminservicev1.UnimplementedAdminServiceServer
	client adminservicev1.AdminServiceClient
}

// NewAdminServiceProxy returns an AdminServiceServer that forwards every
// RPC to client.
func NewAdminServiceProxy(client adminservicev1.AdminServiceClient) *AdminServiceProxy {
	return &AdminServiceProxy{client: client}
}

func (p *AdminServiceProxy) AddOrUpdateRemoteCluster(ctx context.Context, req *adminservicev1.AddOrUpdateRemoteClusterRequest) (*adminservicev1.AddOrUpdateRemoteClusterResponse, error) {
	return forwardUnary(ctx, req, p.client.AddOrUpdateRemoteCluster)
}

func (p *AdminServiceProxy) AddSearchAttributes(ctx context.Context, req *adminservicev1.AddSearchAttributesRequest) (*adminservicev1.AddSearchAttributesResponse, error) {
	return forwardUnary(ctx, req, p.client.AddSearchAttributes)
}

func (p *AdminServiceProxy) AddTasks(ctx context.Context, req *adminservicev1.AddTasksRequest) (*adminservicev1.AddTasksResponse, error) {
	return forwardUnary(ctx, req, p.client.AddTasks)
}

func (p *AdminServiceProxy) CancelDLQJob(ctx context.Context, req *adminservicev1.CancelDLQJobRequest) (*adminservicev1.CancelDLQJobResponse, error) {
	return forwardUnary(ctx, req, p.client.CancelDLQJob)
}

func (p *AdminServiceProxy) CloseShard(ctx context.Context, req *adminservicev1.CloseShardRequest) (*adminservicev1.CloseShardResponse, error) {
	return forwardUnary(ctx, req, p.client.CloseShard)
}

func (p *AdminServiceProxy) DeepHealthCheck(ctx context.Context, req *adminservicev1.DeepHealthCheckRequest) (*adminservicev1.DeepHealthCheckResponse, error) {
	return forwardUnary(ctx, req, p.client.DeepHealthCheck)
}

func (p *AdminServiceProxy) DeleteWorkflowExecution(ctx context.Context, req *adminservicev1.DeleteWorkflowExecutionRequest) (*adminservicev1.DeleteWorkflowExecutionResponse, error) {
	return forwardUnary(ctx, req, p.client.DeleteWorkflowExecution)
}

func (p *AdminServiceProxy) DescribeCluster(ctx context.Context, req *adminservicev1.DescribeClusterRequest) (*adminservicev1.DescribeClusterResponse, error) {
	return forwardUnary(ctx, req, p.client.DescribeCluster)
}

func (p *AdminServiceProxy) DescribeDLQJob(ctx context.Context, req *adminservicev1.DescribeDLQJobRequest) (*adminservicev1.DescribeDLQJobResponse, error) {
	return forwardUnary(ctx, req, p.client.DescribeDLQJob)
}

func (p *AdminServiceProxy) DescribeHistoryHost(ctx context.Context, req *adminservicev1.DescribeHistoryHostRequest) (*adminservicev1.DescribeHistoryHostResponse, error) {
	return forwardUnary(ctx, req, p.client.DescribeHistoryHost)
}

func (p *AdminServiceProxy) DescribeMutableState(ctx context.Context, req *adminservicev1.DescribeMutableStateRequest) (*adminservicev1.DescribeMutableStateResponse, error) {
	return forwardUnary(ctx, req, p.client.DescribeMutableState)
}

func (p *AdminServiceProxy) DescribeTaskQueuePartition(ctx context.Context, req *adminservicev1.DescribeTaskQueuePartitionRequest) (*adminservicev1.DescribeTaskQueuePartitionResponse, error) {
	return forwardUnary(ctx, req, p.client.DescribeTaskQueuePartition)
}

func (p *AdminServiceProxy) ForceUnloadTaskQueuePartition(ctx context.Context, req *adminservicev1.ForceUnloadTaskQueuePartitionRequest) (*adminservicev1.ForceUnloadTaskQueuePartitionResponse, error) {
	return forwardUnary(ctx, req, p.client.ForceUnloadTaskQueuePartition)
}

func (p *AdminServiceProxy) GenerateLastHistoryReplicationTasks(ctx context.Context, req *adminservicev1.GenerateLastHistoryReplicationTasksRequest) (*adminservicev1.GenerateLastHistoryReplicationTasksResponse, error) {
	return forwardUnary(ctx, req, p.client.GenerateLastHistoryReplicationTasks)
}

func (p *AdminServiceProxy) GetDLQMessages(ctx context.Context, req *adminservicev1.GetDLQMessagesRequest) (*adminservicev1.GetDLQMessagesResponse, error) {
	return forwardUnary(ctx, req, p.client.GetDLQMessages)
}

func (p *AdminServiceProxy) GetDLQReplicationMessages(ctx context.Context, req *adminservicev1.GetDLQReplicationMessagesRequest) (*adminservicev1.GetDLQReplicationMessagesResponse, error) {
	return forwardUnary(ctx, req, p.client.GetDLQReplicationMessages)
}

func (p *AdminServiceProxy) GetDLQTasks(ctx context.Context, req *adminservicev1.GetDLQTasksRequest) (*adminservicev1.GetDLQTasksResponse, error) {
	return forwardUnary(ctx, req, p.client.GetDLQTasks)
}

func (p *AdminServiceProxy) GetNamespace(ctx context.Context, req *adminservicev1.GetNamespaceRequest) (*adminservicev1.GetNamespaceResponse, error) {
	return forwardUnary(ctx, req, p.client.GetNamespace)
}

func (p *AdminServiceProxy) GetNamespaceReplicationMessages(ctx context.Context, req *adminservicev1.GetNamespaceReplicationMessagesRequest) (*adminservicev1.GetNamespaceReplicationMessagesResponse, error) {
	return forwardUnary(ctx, req, p.client.GetNamespaceReplicationMessages)
}

func (p *AdminServiceProxy) GetReplicationMessages(ctx context.Context, req *adminservicev1.GetReplicationMessagesRequest) (*adminservicev1.GetReplicationMessagesResponse, error) {
	return forwardUnary(ctx, req, p.client.GetReplicationMessages)
}

func (p *AdminServiceProxy) GetSearchAttributes(ctx context.Context, req *adminservicev1.GetSearchAttributesRequest) (*adminservicev1.GetSearchAttributesResponse, error) {
	return forwardUnary(ctx, req, p.client.GetSearchAttributes)
}

func (p *AdminServiceProxy) GetShard(ctx context.Context, req *adminservicev1.GetShardRequest) (*adminservicev1.GetShardResponse, error) {
	return forwardUnary(ctx, req, p.client.GetShard)
}

func (p *AdminServiceProxy) GetTaskQueueTasks(ctx context.Context, req *adminservicev1.GetTaskQueueTasksRequest) (*adminservicev1.GetTaskQueueTasksResponse, error) {
	return forwardUnary(ctx, req, p.client.GetTaskQueueTasks)
}

func (p *AdminServiceProxy) GetWorkflowExecutionRawHistory(ctx context.Context, req *adminservicev1.GetWorkflowExecutionRawHistoryRequest) (*adminservicev1.GetWorkflowExecutionRawHistoryResponse, error) {
	return forwardUnary(ctx, req, p.client.GetWorkflowExecutionRawHistory)
}

func (p *AdminServiceProxy) GetWorkflowExecutionRawHistoryV2(ctx context.Context, req *adminservicev1.GetWorkflowExecutionRawHistoryV2Request) (*adminservicev1.GetWorkflowExecutionRawHistoryV2Response, error) {
	return forwardUnary(ctx, req, p.client.GetWorkflowExecutionRawHistoryV2)
}

func (p *AdminServiceProxy) ImportWorkflowExecution(ctx context.Context, req *adminservicev1.ImportWorkflowExecutionRequest) (*adminservicev1.ImportWorkflowExecutionResponse, error) {
	return forwardUnary(ctx, req, p.client.ImportWorkflowExecution)
}

func (p *AdminServiceProxy) ListClusterMembers(ctx context.Context, req *adminservicev1.ListClusterMembersRequest) (*adminservicev1.ListClusterMembersResponse, error) {
	return forwardUnary(ctx, req, p.client.ListClusterMembers)
}

func (p *AdminServiceProxy) ListClusters(ctx context.Context, req *adminservicev1.ListClustersRequest) (*adminservicev1.ListClustersResponse, error) {
	return forwardUnary(ctx, req, p.client.ListClusters)
}

func (p *AdminServiceProxy) ListHistoryTasks(ctx context.Context, req *adminservicev1.ListHistoryTasksRequest) (*adminservicev1.ListHistoryTasksResponse, error) {
	return forwardUnary(ctx, req, p.client.ListHistoryTasks)
}

func (p *AdminServiceProxy) ListQueues(ctx context.Context, req *adminservicev1.ListQueuesRequest) (*adminservicev1.ListQueuesResponse, error) {
	return forwardUnary(ctx, req, p.client.ListQueues)
}

func (p *AdminServiceProxy) MergeDLQMessages(ctx context.Context, req *adminservicev1.MergeDLQMessagesRequest) (*adminservicev1.MergeDLQMessagesResponse, error) {
	return forwardUnary(ctx, req, p.client.MergeDLQMessages)
}

func (p *AdminServiceProxy) MergeDLQTasks(ctx context.Context, req *adminservicev1.MergeDLQTasksRequest) (*adminservicev1.MergeDLQTasksResponse, error) {
	return forwardUnary(ctx, req, p.client.MergeDLQTasks)
}

func (p *AdminServiceProxy) PurgeDLQMessages(ctx context.Context, req *adminservicev1.PurgeDLQMessagesRequest) (*adminservicev1.PurgeDLQMessagesResponse, error) {
	return forwardUnary(ctx, req, p.client.PurgeDLQMessages)
}

func (p *AdminServiceProxy) PurgeDLQTasks(ctx context.Context, req *adminservicev1.PurgeDLQTasksRequest) (*adminservicev1.PurgeDLQTasksResponse, error) {
	return forwardUnary(ctx, req, p.client.PurgeDLQTasks)
}

func (p *AdminServiceProxy) ReapplyEvents(ctx context.Context, req *adminservicev1.ReapplyEventsRequest) (*adminservicev1.ReapplyEventsResponse, error) {
	return forwardUnary(ctx, req, p.client.ReapplyEvents)
}

func (p *AdminServiceProxy) RebuildMutableState(ctx context.Context, req *adminservicev1.RebuildMutableStateRequest) (*adminservicev1.RebuildMutableStateResponse, error) {
	return forwardUnary(ctx, req, p.client.RebuildMutableState)
}

func (p *AdminServiceProxy) RefreshWorkflowTasks(ctx context.Context, req *adminservicev1.RefreshWorkflowTasksRequest) (*adminservicev1.RefreshWorkflowTasksResponse, error) {
	return forwardUnary(ctx, req, p.client.RefreshWorkflowTasks)
}

func (p *AdminServiceProxy) RemoveRemoteCluster(ctx context.Context, req *adminservicev1.RemoveRemoteClusterRequest) (*adminservicev1.RemoveRemoteClusterResponse, error) {
	return forwardUnary(ctx, req, p.client.RemoveRemoteCluster)
}

func (p *AdminServiceProxy) RemoveSearchAttributes(ctx context.Context, req *adminservicev1.RemoveSearchAttributesRequest) (*adminservicev1.RemoveSearchAttributesResponse, error) {
	return forwardUnary(ctx, req, p.client.RemoveSearchAttributes)
}

func (p *AdminServiceProxy) RemoveTask(ctx context.Context, req *adminservicev1.RemoveTaskRequest) (*adminservicev1.RemoveTaskResponse, error) {
	return forwardUnary(ctx, req, p.client.RemoveTask)
}

func (p *AdminServiceProxy) ResendReplicationTasks(ctx context.Context, req *adminservicev1.ResendReplicationTasksRequest) (*adminservicev1.ResendReplicationTasksResponse, error) {
	return forwardUnary(ctx, req, p.client.ResendReplicationTasks)
}

func (p *AdminServiceProxy) StartAdminBatchOperation(ctx context.Context, req *adminservicev1.StartAdminBatchOperationRequest) (*adminservicev1.StartAdminBatchOperationResponse, error) {
	return forwardUnary(ctx, req, p.client.StartAdminBatchOperation)
}

func (p *AdminServiceProxy) StreamWorkflowReplicationMessages(stream adminservicev1.AdminService_StreamWorkflowReplicationMessagesServer) error {
	return forwardBidiStream(stream, p.client.StreamWorkflowReplicationMessages)
}

func (p *AdminServiceProxy) SyncWorkflowState(ctx context.Context, req *adminservicev1.SyncWorkflowStateRequest) (*adminservicev1.SyncWorkflowStateResponse, error) {
	return forwardUnary(ctx, req, p.client.SyncWorkflowState)
}
