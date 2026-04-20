package proxy

import (
	"context"

	workflowservicev1 "go.temporal.io/api/workflowservice/v1"
)

type WorkflowServiceProxy struct {
	workflowservicev1.UnimplementedWorkflowServiceServer
	client workflowservicev1.WorkflowServiceClient
}

// NewWorkflowServiceProxy returns a WorkflowServiceServer that forwards every
// RPC to client via forwardUnary.
func NewWorkflowServiceProxy(client workflowservicev1.WorkflowServiceClient) *WorkflowServiceProxy {
	return &WorkflowServiceProxy{client: client}
}

func (p *WorkflowServiceProxy) CountActivityExecutions(ctx context.Context, req *workflowservicev1.CountActivityExecutionsRequest) (*workflowservicev1.CountActivityExecutionsResponse, error) {
	return forwardUnary(ctx, req, p.client.CountActivityExecutions)
}

func (p *WorkflowServiceProxy) CountSchedules(ctx context.Context, req *workflowservicev1.CountSchedulesRequest) (*workflowservicev1.CountSchedulesResponse, error) {
	return forwardUnary(ctx, req, p.client.CountSchedules)
}

func (p *WorkflowServiceProxy) CountWorkflowExecutions(ctx context.Context, req *workflowservicev1.CountWorkflowExecutionsRequest) (*workflowservicev1.CountWorkflowExecutionsResponse, error) {
	return forwardUnary(ctx, req, p.client.CountWorkflowExecutions)
}

func (p *WorkflowServiceProxy) CreateSchedule(ctx context.Context, req *workflowservicev1.CreateScheduleRequest) (*workflowservicev1.CreateScheduleResponse, error) {
	return forwardUnary(ctx, req, p.client.CreateSchedule)
}

func (p *WorkflowServiceProxy) CreateWorkflowRule(ctx context.Context, req *workflowservicev1.CreateWorkflowRuleRequest) (*workflowservicev1.CreateWorkflowRuleResponse, error) {
	return forwardUnary(ctx, req, p.client.CreateWorkflowRule)
}

func (p *WorkflowServiceProxy) DeleteActivityExecution(ctx context.Context, req *workflowservicev1.DeleteActivityExecutionRequest) (*workflowservicev1.DeleteActivityExecutionResponse, error) {
	return forwardUnary(ctx, req, p.client.DeleteActivityExecution)
}

func (p *WorkflowServiceProxy) DeleteSchedule(ctx context.Context, req *workflowservicev1.DeleteScheduleRequest) (*workflowservicev1.DeleteScheduleResponse, error) {
	return forwardUnary(ctx, req, p.client.DeleteSchedule)
}

func (p *WorkflowServiceProxy) DeleteWorkerDeployment(ctx context.Context, req *workflowservicev1.DeleteWorkerDeploymentRequest) (*workflowservicev1.DeleteWorkerDeploymentResponse, error) {
	return forwardUnary(ctx, req, p.client.DeleteWorkerDeployment)
}

func (p *WorkflowServiceProxy) DeleteWorkerDeploymentVersion(ctx context.Context, req *workflowservicev1.DeleteWorkerDeploymentVersionRequest) (*workflowservicev1.DeleteWorkerDeploymentVersionResponse, error) {
	return forwardUnary(ctx, req, p.client.DeleteWorkerDeploymentVersion)
}

func (p *WorkflowServiceProxy) DeleteWorkflowExecution(ctx context.Context, req *workflowservicev1.DeleteWorkflowExecutionRequest) (*workflowservicev1.DeleteWorkflowExecutionResponse, error) {
	return forwardUnary(ctx, req, p.client.DeleteWorkflowExecution)
}

func (p *WorkflowServiceProxy) DeleteWorkflowRule(ctx context.Context, req *workflowservicev1.DeleteWorkflowRuleRequest) (*workflowservicev1.DeleteWorkflowRuleResponse, error) {
	return forwardUnary(ctx, req, p.client.DeleteWorkflowRule)
}

func (p *WorkflowServiceProxy) DeprecateNamespace(ctx context.Context, req *workflowservicev1.DeprecateNamespaceRequest) (*workflowservicev1.DeprecateNamespaceResponse, error) {
	return forwardUnary(ctx, req, p.client.DeprecateNamespace)
}

func (p *WorkflowServiceProxy) DescribeActivityExecution(ctx context.Context, req *workflowservicev1.DescribeActivityExecutionRequest) (*workflowservicev1.DescribeActivityExecutionResponse, error) {
	return forwardUnary(ctx, req, p.client.DescribeActivityExecution)
}

func (p *WorkflowServiceProxy) DescribeBatchOperation(ctx context.Context, req *workflowservicev1.DescribeBatchOperationRequest) (*workflowservicev1.DescribeBatchOperationResponse, error) {
	return forwardUnary(ctx, req, p.client.DescribeBatchOperation)
}

func (p *WorkflowServiceProxy) DescribeDeployment(ctx context.Context, req *workflowservicev1.DescribeDeploymentRequest) (*workflowservicev1.DescribeDeploymentResponse, error) {
	return forwardUnary(ctx, req, p.client.DescribeDeployment)
}

func (p *WorkflowServiceProxy) DescribeNamespace(ctx context.Context, req *workflowservicev1.DescribeNamespaceRequest) (*workflowservicev1.DescribeNamespaceResponse, error) {
	return forwardUnary(ctx, req, p.client.DescribeNamespace)
}

func (p *WorkflowServiceProxy) DescribeSchedule(ctx context.Context, req *workflowservicev1.DescribeScheduleRequest) (*workflowservicev1.DescribeScheduleResponse, error) {
	return forwardUnary(ctx, req, p.client.DescribeSchedule)
}

func (p *WorkflowServiceProxy) DescribeTaskQueue(ctx context.Context, req *workflowservicev1.DescribeTaskQueueRequest) (*workflowservicev1.DescribeTaskQueueResponse, error) {
	return forwardUnary(ctx, req, p.client.DescribeTaskQueue)
}

func (p *WorkflowServiceProxy) DescribeWorker(ctx context.Context, req *workflowservicev1.DescribeWorkerRequest) (*workflowservicev1.DescribeWorkerResponse, error) {
	return forwardUnary(ctx, req, p.client.DescribeWorker)
}

func (p *WorkflowServiceProxy) DescribeWorkerDeployment(ctx context.Context, req *workflowservicev1.DescribeWorkerDeploymentRequest) (*workflowservicev1.DescribeWorkerDeploymentResponse, error) {
	return forwardUnary(ctx, req, p.client.DescribeWorkerDeployment)
}

func (p *WorkflowServiceProxy) DescribeWorkerDeploymentVersion(ctx context.Context, req *workflowservicev1.DescribeWorkerDeploymentVersionRequest) (*workflowservicev1.DescribeWorkerDeploymentVersionResponse, error) {
	return forwardUnary(ctx, req, p.client.DescribeWorkerDeploymentVersion)
}

func (p *WorkflowServiceProxy) DescribeWorkflowExecution(ctx context.Context, req *workflowservicev1.DescribeWorkflowExecutionRequest) (*workflowservicev1.DescribeWorkflowExecutionResponse, error) {
	return forwardUnary(ctx, req, p.client.DescribeWorkflowExecution)
}

func (p *WorkflowServiceProxy) DescribeWorkflowRule(ctx context.Context, req *workflowservicev1.DescribeWorkflowRuleRequest) (*workflowservicev1.DescribeWorkflowRuleResponse, error) {
	return forwardUnary(ctx, req, p.client.DescribeWorkflowRule)
}

func (p *WorkflowServiceProxy) ExecuteMultiOperation(ctx context.Context, req *workflowservicev1.ExecuteMultiOperationRequest) (*workflowservicev1.ExecuteMultiOperationResponse, error) {
	return forwardUnary(ctx, req, p.client.ExecuteMultiOperation)
}

func (p *WorkflowServiceProxy) FetchWorkerConfig(ctx context.Context, req *workflowservicev1.FetchWorkerConfigRequest) (*workflowservicev1.FetchWorkerConfigResponse, error) {
	return forwardUnary(ctx, req, p.client.FetchWorkerConfig)
}

func (p *WorkflowServiceProxy) GetClusterInfo(ctx context.Context, req *workflowservicev1.GetClusterInfoRequest) (*workflowservicev1.GetClusterInfoResponse, error) {
	return forwardUnary(ctx, req, p.client.GetClusterInfo)
}

func (p *WorkflowServiceProxy) GetCurrentDeployment(ctx context.Context, req *workflowservicev1.GetCurrentDeploymentRequest) (*workflowservicev1.GetCurrentDeploymentResponse, error) {
	return forwardUnary(ctx, req, p.client.GetCurrentDeployment)
}

func (p *WorkflowServiceProxy) GetDeploymentReachability(ctx context.Context, req *workflowservicev1.GetDeploymentReachabilityRequest) (*workflowservicev1.GetDeploymentReachabilityResponse, error) {
	return forwardUnary(ctx, req, p.client.GetDeploymentReachability)
}

func (p *WorkflowServiceProxy) GetSearchAttributes(ctx context.Context, req *workflowservicev1.GetSearchAttributesRequest) (*workflowservicev1.GetSearchAttributesResponse, error) {
	return forwardUnary(ctx, req, p.client.GetSearchAttributes)
}

func (p *WorkflowServiceProxy) GetSystemInfo(ctx context.Context, req *workflowservicev1.GetSystemInfoRequest) (*workflowservicev1.GetSystemInfoResponse, error) {
	return forwardUnary(ctx, req, p.client.GetSystemInfo)
}

func (p *WorkflowServiceProxy) GetWorkerBuildIdCompatibility(ctx context.Context, req *workflowservicev1.GetWorkerBuildIdCompatibilityRequest) (*workflowservicev1.GetWorkerBuildIdCompatibilityResponse, error) {
	return forwardUnary(ctx, req, p.client.GetWorkerBuildIdCompatibility)
}

func (p *WorkflowServiceProxy) GetWorkerTaskReachability(ctx context.Context, req *workflowservicev1.GetWorkerTaskReachabilityRequest) (*workflowservicev1.GetWorkerTaskReachabilityResponse, error) {
	return forwardUnary(ctx, req, p.client.GetWorkerTaskReachability)
}

func (p *WorkflowServiceProxy) GetWorkerVersioningRules(ctx context.Context, req *workflowservicev1.GetWorkerVersioningRulesRequest) (*workflowservicev1.GetWorkerVersioningRulesResponse, error) {
	return forwardUnary(ctx, req, p.client.GetWorkerVersioningRules)
}

func (p *WorkflowServiceProxy) GetWorkflowExecutionHistory(ctx context.Context, req *workflowservicev1.GetWorkflowExecutionHistoryRequest) (*workflowservicev1.GetWorkflowExecutionHistoryResponse, error) {
	return forwardUnary(ctx, req, p.client.GetWorkflowExecutionHistory)
}

func (p *WorkflowServiceProxy) GetWorkflowExecutionHistoryReverse(ctx context.Context, req *workflowservicev1.GetWorkflowExecutionHistoryReverseRequest) (*workflowservicev1.GetWorkflowExecutionHistoryReverseResponse, error) {
	return forwardUnary(ctx, req, p.client.GetWorkflowExecutionHistoryReverse)
}

func (p *WorkflowServiceProxy) ListActivityExecutions(ctx context.Context, req *workflowservicev1.ListActivityExecutionsRequest) (*workflowservicev1.ListActivityExecutionsResponse, error) {
	return forwardUnary(ctx, req, p.client.ListActivityExecutions)
}

func (p *WorkflowServiceProxy) ListArchivedWorkflowExecutions(ctx context.Context, req *workflowservicev1.ListArchivedWorkflowExecutionsRequest) (*workflowservicev1.ListArchivedWorkflowExecutionsResponse, error) {
	return forwardUnary(ctx, req, p.client.ListArchivedWorkflowExecutions)
}

func (p *WorkflowServiceProxy) ListBatchOperations(ctx context.Context, req *workflowservicev1.ListBatchOperationsRequest) (*workflowservicev1.ListBatchOperationsResponse, error) {
	return forwardUnary(ctx, req, p.client.ListBatchOperations)
}

func (p *WorkflowServiceProxy) ListClosedWorkflowExecutions(ctx context.Context, req *workflowservicev1.ListClosedWorkflowExecutionsRequest) (*workflowservicev1.ListClosedWorkflowExecutionsResponse, error) {
	return forwardUnary(ctx, req, p.client.ListClosedWorkflowExecutions)
}

func (p *WorkflowServiceProxy) ListDeployments(ctx context.Context, req *workflowservicev1.ListDeploymentsRequest) (*workflowservicev1.ListDeploymentsResponse, error) {
	return forwardUnary(ctx, req, p.client.ListDeployments)
}

func (p *WorkflowServiceProxy) ListNamespaces(ctx context.Context, req *workflowservicev1.ListNamespacesRequest) (*workflowservicev1.ListNamespacesResponse, error) {
	return forwardUnary(ctx, req, p.client.ListNamespaces)
}

func (p *WorkflowServiceProxy) ListOpenWorkflowExecutions(ctx context.Context, req *workflowservicev1.ListOpenWorkflowExecutionsRequest) (*workflowservicev1.ListOpenWorkflowExecutionsResponse, error) {
	return forwardUnary(ctx, req, p.client.ListOpenWorkflowExecutions)
}

func (p *WorkflowServiceProxy) ListScheduleMatchingTimes(ctx context.Context, req *workflowservicev1.ListScheduleMatchingTimesRequest) (*workflowservicev1.ListScheduleMatchingTimesResponse, error) {
	return forwardUnary(ctx, req, p.client.ListScheduleMatchingTimes)
}

func (p *WorkflowServiceProxy) ListSchedules(ctx context.Context, req *workflowservicev1.ListSchedulesRequest) (*workflowservicev1.ListSchedulesResponse, error) {
	return forwardUnary(ctx, req, p.client.ListSchedules)
}

func (p *WorkflowServiceProxy) ListTaskQueuePartitions(ctx context.Context, req *workflowservicev1.ListTaskQueuePartitionsRequest) (*workflowservicev1.ListTaskQueuePartitionsResponse, error) {
	return forwardUnary(ctx, req, p.client.ListTaskQueuePartitions)
}

func (p *WorkflowServiceProxy) ListWorkerDeployments(ctx context.Context, req *workflowservicev1.ListWorkerDeploymentsRequest) (*workflowservicev1.ListWorkerDeploymentsResponse, error) {
	return forwardUnary(ctx, req, p.client.ListWorkerDeployments)
}

func (p *WorkflowServiceProxy) ListWorkers(ctx context.Context, req *workflowservicev1.ListWorkersRequest) (*workflowservicev1.ListWorkersResponse, error) {
	return forwardUnary(ctx, req, p.client.ListWorkers)
}

func (p *WorkflowServiceProxy) ListWorkflowExecutions(ctx context.Context, req *workflowservicev1.ListWorkflowExecutionsRequest) (*workflowservicev1.ListWorkflowExecutionsResponse, error) {
	return forwardUnary(ctx, req, p.client.ListWorkflowExecutions)
}

func (p *WorkflowServiceProxy) ListWorkflowRules(ctx context.Context, req *workflowservicev1.ListWorkflowRulesRequest) (*workflowservicev1.ListWorkflowRulesResponse, error) {
	return forwardUnary(ctx, req, p.client.ListWorkflowRules)
}

func (p *WorkflowServiceProxy) PatchSchedule(ctx context.Context, req *workflowservicev1.PatchScheduleRequest) (*workflowservicev1.PatchScheduleResponse, error) {
	return forwardUnary(ctx, req, p.client.PatchSchedule)
}

func (p *WorkflowServiceProxy) PauseActivity(ctx context.Context, req *workflowservicev1.PauseActivityRequest) (*workflowservicev1.PauseActivityResponse, error) {
	return forwardUnary(ctx, req, p.client.PauseActivity)
}

func (p *WorkflowServiceProxy) PauseWorkflowExecution(ctx context.Context, req *workflowservicev1.PauseWorkflowExecutionRequest) (*workflowservicev1.PauseWorkflowExecutionResponse, error) {
	return forwardUnary(ctx, req, p.client.PauseWorkflowExecution)
}

func (p *WorkflowServiceProxy) PollActivityExecution(ctx context.Context, req *workflowservicev1.PollActivityExecutionRequest) (*workflowservicev1.PollActivityExecutionResponse, error) {
	return forwardUnary(ctx, req, p.client.PollActivityExecution)
}

func (p *WorkflowServiceProxy) PollActivityTaskQueue(ctx context.Context, req *workflowservicev1.PollActivityTaskQueueRequest) (*workflowservicev1.PollActivityTaskQueueResponse, error) {
	return forwardUnary(ctx, req, p.client.PollActivityTaskQueue)
}

func (p *WorkflowServiceProxy) PollNexusTaskQueue(ctx context.Context, req *workflowservicev1.PollNexusTaskQueueRequest) (*workflowservicev1.PollNexusTaskQueueResponse, error) {
	return forwardUnary(ctx, req, p.client.PollNexusTaskQueue)
}

func (p *WorkflowServiceProxy) PollWorkflowExecutionUpdate(ctx context.Context, req *workflowservicev1.PollWorkflowExecutionUpdateRequest) (*workflowservicev1.PollWorkflowExecutionUpdateResponse, error) {
	return forwardUnary(ctx, req, p.client.PollWorkflowExecutionUpdate)
}

func (p *WorkflowServiceProxy) PollWorkflowTaskQueue(ctx context.Context, req *workflowservicev1.PollWorkflowTaskQueueRequest) (*workflowservicev1.PollWorkflowTaskQueueResponse, error) {
	return forwardUnary(ctx, req, p.client.PollWorkflowTaskQueue)
}

func (p *WorkflowServiceProxy) QueryWorkflow(ctx context.Context, req *workflowservicev1.QueryWorkflowRequest) (*workflowservicev1.QueryWorkflowResponse, error) {
	return forwardUnary(ctx, req, p.client.QueryWorkflow)
}

func (p *WorkflowServiceProxy) RecordActivityTaskHeartbeat(ctx context.Context, req *workflowservicev1.RecordActivityTaskHeartbeatRequest) (*workflowservicev1.RecordActivityTaskHeartbeatResponse, error) {
	return forwardUnary(ctx, req, p.client.RecordActivityTaskHeartbeat)
}

func (p *WorkflowServiceProxy) RecordActivityTaskHeartbeatById(ctx context.Context, req *workflowservicev1.RecordActivityTaskHeartbeatByIdRequest) (*workflowservicev1.RecordActivityTaskHeartbeatByIdResponse, error) {
	return forwardUnary(ctx, req, p.client.RecordActivityTaskHeartbeatById)
}

func (p *WorkflowServiceProxy) RecordWorkerHeartbeat(ctx context.Context, req *workflowservicev1.RecordWorkerHeartbeatRequest) (*workflowservicev1.RecordWorkerHeartbeatResponse, error) {
	return forwardUnary(ctx, req, p.client.RecordWorkerHeartbeat)
}

func (p *WorkflowServiceProxy) RegisterNamespace(ctx context.Context, req *workflowservicev1.RegisterNamespaceRequest) (*workflowservicev1.RegisterNamespaceResponse, error) {
	return forwardUnary(ctx, req, p.client.RegisterNamespace)
}

func (p *WorkflowServiceProxy) RequestCancelActivityExecution(ctx context.Context, req *workflowservicev1.RequestCancelActivityExecutionRequest) (*workflowservicev1.RequestCancelActivityExecutionResponse, error) {
	return forwardUnary(ctx, req, p.client.RequestCancelActivityExecution)
}

func (p *WorkflowServiceProxy) RequestCancelWorkflowExecution(ctx context.Context, req *workflowservicev1.RequestCancelWorkflowExecutionRequest) (*workflowservicev1.RequestCancelWorkflowExecutionResponse, error) {
	return forwardUnary(ctx, req, p.client.RequestCancelWorkflowExecution)
}

func (p *WorkflowServiceProxy) ResetActivity(ctx context.Context, req *workflowservicev1.ResetActivityRequest) (*workflowservicev1.ResetActivityResponse, error) {
	return forwardUnary(ctx, req, p.client.ResetActivity)
}

func (p *WorkflowServiceProxy) ResetStickyTaskQueue(ctx context.Context, req *workflowservicev1.ResetStickyTaskQueueRequest) (*workflowservicev1.ResetStickyTaskQueueResponse, error) {
	return forwardUnary(ctx, req, p.client.ResetStickyTaskQueue)
}

func (p *WorkflowServiceProxy) ResetWorkflowExecution(ctx context.Context, req *workflowservicev1.ResetWorkflowExecutionRequest) (*workflowservicev1.ResetWorkflowExecutionResponse, error) {
	return forwardUnary(ctx, req, p.client.ResetWorkflowExecution)
}

func (p *WorkflowServiceProxy) RespondActivityTaskCanceled(ctx context.Context, req *workflowservicev1.RespondActivityTaskCanceledRequest) (*workflowservicev1.RespondActivityTaskCanceledResponse, error) {
	return forwardUnary(ctx, req, p.client.RespondActivityTaskCanceled)
}

func (p *WorkflowServiceProxy) RespondActivityTaskCanceledById(ctx context.Context, req *workflowservicev1.RespondActivityTaskCanceledByIdRequest) (*workflowservicev1.RespondActivityTaskCanceledByIdResponse, error) {
	return forwardUnary(ctx, req, p.client.RespondActivityTaskCanceledById)
}

func (p *WorkflowServiceProxy) RespondActivityTaskCompleted(ctx context.Context, req *workflowservicev1.RespondActivityTaskCompletedRequest) (*workflowservicev1.RespondActivityTaskCompletedResponse, error) {
	return forwardUnary(ctx, req, p.client.RespondActivityTaskCompleted)
}

func (p *WorkflowServiceProxy) RespondActivityTaskCompletedById(ctx context.Context, req *workflowservicev1.RespondActivityTaskCompletedByIdRequest) (*workflowservicev1.RespondActivityTaskCompletedByIdResponse, error) {
	return forwardUnary(ctx, req, p.client.RespondActivityTaskCompletedById)
}

func (p *WorkflowServiceProxy) RespondActivityTaskFailed(ctx context.Context, req *workflowservicev1.RespondActivityTaskFailedRequest) (*workflowservicev1.RespondActivityTaskFailedResponse, error) {
	return forwardUnary(ctx, req, p.client.RespondActivityTaskFailed)
}

func (p *WorkflowServiceProxy) RespondActivityTaskFailedById(ctx context.Context, req *workflowservicev1.RespondActivityTaskFailedByIdRequest) (*workflowservicev1.RespondActivityTaskFailedByIdResponse, error) {
	return forwardUnary(ctx, req, p.client.RespondActivityTaskFailedById)
}

func (p *WorkflowServiceProxy) RespondNexusTaskCompleted(ctx context.Context, req *workflowservicev1.RespondNexusTaskCompletedRequest) (*workflowservicev1.RespondNexusTaskCompletedResponse, error) {
	return forwardUnary(ctx, req, p.client.RespondNexusTaskCompleted)
}

func (p *WorkflowServiceProxy) RespondNexusTaskFailed(ctx context.Context, req *workflowservicev1.RespondNexusTaskFailedRequest) (*workflowservicev1.RespondNexusTaskFailedResponse, error) {
	return forwardUnary(ctx, req, p.client.RespondNexusTaskFailed)
}

func (p *WorkflowServiceProxy) RespondQueryTaskCompleted(ctx context.Context, req *workflowservicev1.RespondQueryTaskCompletedRequest) (*workflowservicev1.RespondQueryTaskCompletedResponse, error) {
	return forwardUnary(ctx, req, p.client.RespondQueryTaskCompleted)
}

func (p *WorkflowServiceProxy) RespondWorkflowTaskCompleted(ctx context.Context, req *workflowservicev1.RespondWorkflowTaskCompletedRequest) (*workflowservicev1.RespondWorkflowTaskCompletedResponse, error) {
	return forwardUnary(ctx, req, p.client.RespondWorkflowTaskCompleted)
}

func (p *WorkflowServiceProxy) RespondWorkflowTaskFailed(ctx context.Context, req *workflowservicev1.RespondWorkflowTaskFailedRequest) (*workflowservicev1.RespondWorkflowTaskFailedResponse, error) {
	return forwardUnary(ctx, req, p.client.RespondWorkflowTaskFailed)
}

func (p *WorkflowServiceProxy) ScanWorkflowExecutions(ctx context.Context, req *workflowservicev1.ScanWorkflowExecutionsRequest) (*workflowservicev1.ScanWorkflowExecutionsResponse, error) { //nolint:staticcheck
	return forwardUnary(ctx, req, p.client.ScanWorkflowExecutions) //nolint:staticcheck
}

func (p *WorkflowServiceProxy) SetCurrentDeployment(ctx context.Context, req *workflowservicev1.SetCurrentDeploymentRequest) (*workflowservicev1.SetCurrentDeploymentResponse, error) {
	return forwardUnary(ctx, req, p.client.SetCurrentDeployment)
}

func (p *WorkflowServiceProxy) SetWorkerDeploymentCurrentVersion(ctx context.Context, req *workflowservicev1.SetWorkerDeploymentCurrentVersionRequest) (*workflowservicev1.SetWorkerDeploymentCurrentVersionResponse, error) {
	return forwardUnary(ctx, req, p.client.SetWorkerDeploymentCurrentVersion)
}

func (p *WorkflowServiceProxy) SetWorkerDeploymentManager(ctx context.Context, req *workflowservicev1.SetWorkerDeploymentManagerRequest) (*workflowservicev1.SetWorkerDeploymentManagerResponse, error) {
	return forwardUnary(ctx, req, p.client.SetWorkerDeploymentManager)
}

func (p *WorkflowServiceProxy) SetWorkerDeploymentRampingVersion(ctx context.Context, req *workflowservicev1.SetWorkerDeploymentRampingVersionRequest) (*workflowservicev1.SetWorkerDeploymentRampingVersionResponse, error) {
	return forwardUnary(ctx, req, p.client.SetWorkerDeploymentRampingVersion)
}

func (p *WorkflowServiceProxy) ShutdownWorker(ctx context.Context, req *workflowservicev1.ShutdownWorkerRequest) (*workflowservicev1.ShutdownWorkerResponse, error) {
	return forwardUnary(ctx, req, p.client.ShutdownWorker)
}

func (p *WorkflowServiceProxy) SignalWithStartWorkflowExecution(ctx context.Context, req *workflowservicev1.SignalWithStartWorkflowExecutionRequest) (*workflowservicev1.SignalWithStartWorkflowExecutionResponse, error) {
	return forwardUnary(ctx, req, p.client.SignalWithStartWorkflowExecution)
}

func (p *WorkflowServiceProxy) SignalWorkflowExecution(ctx context.Context, req *workflowservicev1.SignalWorkflowExecutionRequest) (*workflowservicev1.SignalWorkflowExecutionResponse, error) {
	return forwardUnary(ctx, req, p.client.SignalWorkflowExecution)
}

func (p *WorkflowServiceProxy) StartActivityExecution(ctx context.Context, req *workflowservicev1.StartActivityExecutionRequest) (*workflowservicev1.StartActivityExecutionResponse, error) {
	return forwardUnary(ctx, req, p.client.StartActivityExecution)
}

func (p *WorkflowServiceProxy) StartBatchOperation(ctx context.Context, req *workflowservicev1.StartBatchOperationRequest) (*workflowservicev1.StartBatchOperationResponse, error) {
	return forwardUnary(ctx, req, p.client.StartBatchOperation)
}

func (p *WorkflowServiceProxy) StartWorkflowExecution(ctx context.Context, req *workflowservicev1.StartWorkflowExecutionRequest) (*workflowservicev1.StartWorkflowExecutionResponse, error) {
	return forwardUnary(ctx, req, p.client.StartWorkflowExecution)
}

func (p *WorkflowServiceProxy) StopBatchOperation(ctx context.Context, req *workflowservicev1.StopBatchOperationRequest) (*workflowservicev1.StopBatchOperationResponse, error) {
	return forwardUnary(ctx, req, p.client.StopBatchOperation)
}

func (p *WorkflowServiceProxy) TerminateActivityExecution(ctx context.Context, req *workflowservicev1.TerminateActivityExecutionRequest) (*workflowservicev1.TerminateActivityExecutionResponse, error) {
	return forwardUnary(ctx, req, p.client.TerminateActivityExecution)
}

func (p *WorkflowServiceProxy) TerminateWorkflowExecution(ctx context.Context, req *workflowservicev1.TerminateWorkflowExecutionRequest) (*workflowservicev1.TerminateWorkflowExecutionResponse, error) {
	return forwardUnary(ctx, req, p.client.TerminateWorkflowExecution)
}

func (p *WorkflowServiceProxy) TriggerWorkflowRule(ctx context.Context, req *workflowservicev1.TriggerWorkflowRuleRequest) (*workflowservicev1.TriggerWorkflowRuleResponse, error) {
	return forwardUnary(ctx, req, p.client.TriggerWorkflowRule)
}

func (p *WorkflowServiceProxy) UnpauseActivity(ctx context.Context, req *workflowservicev1.UnpauseActivityRequest) (*workflowservicev1.UnpauseActivityResponse, error) {
	return forwardUnary(ctx, req, p.client.UnpauseActivity)
}

func (p *WorkflowServiceProxy) UnpauseWorkflowExecution(ctx context.Context, req *workflowservicev1.UnpauseWorkflowExecutionRequest) (*workflowservicev1.UnpauseWorkflowExecutionResponse, error) {
	return forwardUnary(ctx, req, p.client.UnpauseWorkflowExecution)
}

func (p *WorkflowServiceProxy) UpdateActivityOptions(ctx context.Context, req *workflowservicev1.UpdateActivityOptionsRequest) (*workflowservicev1.UpdateActivityOptionsResponse, error) {
	return forwardUnary(ctx, req, p.client.UpdateActivityOptions)
}

func (p *WorkflowServiceProxy) UpdateNamespace(ctx context.Context, req *workflowservicev1.UpdateNamespaceRequest) (*workflowservicev1.UpdateNamespaceResponse, error) {
	return forwardUnary(ctx, req, p.client.UpdateNamespace)
}

func (p *WorkflowServiceProxy) UpdateSchedule(ctx context.Context, req *workflowservicev1.UpdateScheduleRequest) (*workflowservicev1.UpdateScheduleResponse, error) {
	return forwardUnary(ctx, req, p.client.UpdateSchedule)
}

func (p *WorkflowServiceProxy) UpdateTaskQueueConfig(ctx context.Context, req *workflowservicev1.UpdateTaskQueueConfigRequest) (*workflowservicev1.UpdateTaskQueueConfigResponse, error) {
	return forwardUnary(ctx, req, p.client.UpdateTaskQueueConfig)
}

func (p *WorkflowServiceProxy) UpdateWorkerBuildIdCompatibility(ctx context.Context, req *workflowservicev1.UpdateWorkerBuildIdCompatibilityRequest) (*workflowservicev1.UpdateWorkerBuildIdCompatibilityResponse, error) {
	return forwardUnary(ctx, req, p.client.UpdateWorkerBuildIdCompatibility)
}

func (p *WorkflowServiceProxy) UpdateWorkerConfig(ctx context.Context, req *workflowservicev1.UpdateWorkerConfigRequest) (*workflowservicev1.UpdateWorkerConfigResponse, error) {
	return forwardUnary(ctx, req, p.client.UpdateWorkerConfig)
}

func (p *WorkflowServiceProxy) UpdateWorkerDeploymentVersionMetadata(ctx context.Context, req *workflowservicev1.UpdateWorkerDeploymentVersionMetadataRequest) (*workflowservicev1.UpdateWorkerDeploymentVersionMetadataResponse, error) {
	return forwardUnary(ctx, req, p.client.UpdateWorkerDeploymentVersionMetadata)
}

func (p *WorkflowServiceProxy) UpdateWorkerVersioningRules(ctx context.Context, req *workflowservicev1.UpdateWorkerVersioningRulesRequest) (*workflowservicev1.UpdateWorkerVersioningRulesResponse, error) {
	return forwardUnary(ctx, req, p.client.UpdateWorkerVersioningRules)
}

func (p *WorkflowServiceProxy) UpdateWorkflowExecution(ctx context.Context, req *workflowservicev1.UpdateWorkflowExecutionRequest) (*workflowservicev1.UpdateWorkflowExecutionResponse, error) {
	return forwardUnary(ctx, req, p.client.UpdateWorkflowExecution)
}

func (p *WorkflowServiceProxy) UpdateWorkflowExecutionOptions(ctx context.Context, req *workflowservicev1.UpdateWorkflowExecutionOptionsRequest) (*workflowservicev1.UpdateWorkflowExecutionOptionsResponse, error) {
	return forwardUnary(ctx, req, p.client.UpdateWorkflowExecutionOptions)
}
