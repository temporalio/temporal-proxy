package translation

import (
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	namespacepb "go.temporal.io/api/namespace/v1"
	workflowservice "go.temporal.io/api/workflowservice/v1"
	cloudservice "go.temporal.io/cloud-sdk/api/cloudservice/v1"
	cloudnamespace "go.temporal.io/cloud-sdk/api/namespace/v1"
	"go.temporal.io/cloud-sdk/api/resource/v1"
	"go.temporal.io/cloud-sdk/cloudclient"
	"google.golang.org/protobuf/types/known/durationpb"

	"github.com/temporalio/temporal-proxy/internal/services"
)

const (
	// listNamespacesMethod is the WorkflowService method local callers use to list
	// namespaces, and getNamespacesMethod is the CloudService method that answers
	// it on Temporal Cloud, whose frontends do not serve the namespace list.
	listNamespacesMethod = "/" + services.WorkflowService + "/ListNamespaces"
	getNamespacesMethod  = "/" + services.CloudService + "/GetNamespaces"

	// cloudPageLimit is the largest page CloudService.GetNamespaces accepts. A
	// larger request is rejected outright, so a caller asking for more is clamped
	// to it and paginates instead.
	cloudPageLimit = 1000

	// hoursPerDay converts the retention Cloud reports in whole days into the
	// duration a Temporal namespace config carries.
	hoursPerDay = 24 * time.Hour
)

// listNamespaces translates WorkflowService.ListNamespaces onto
// CloudService.GetNamespaces.
//
// The Cloud API version header is required - without it GetNamespaces fails with
// InvalidArgument - and is pinned to the version the SDK this package compiles
// against defaults to, since that is the same module the message types and their
// versioned fields come from. Bumping go.temporal.io/cloud-sdk moves the
// conversions and the version they were written against together.
func listNamespaces() *Translation {
	return Adapt(listNamespacesMethod, getNamespacesMethod, listNamespacesRequest, listNamespacesResponse).
		WithHeader(cloudclient.TemporalCloudAPIVersionHeader(), cloudclient.DefaultAPIVersion())
}

// listNamespacesRequest converts a ListNamespaces request into the GetNamespaces
// request that stands in for it.
//
// The page token is opaque to the caller and is carried across verbatim: Cloud
// spells it as a string where WorkflowService spells it as bytes, and the token
// a caller sends back is one this translation handed it in the first place. The
// namespace filter has no equivalent on the Cloud request and is applied to the
// response instead.
func listNamespacesRequest(req *workflowservice.ListNamespacesRequest) (*cloudservice.GetNamespacesRequest, error) {
	return &cloudservice.GetNamespacesRequest{
		PageSize:  min(req.GetPageSize(), cloudPageLimit),
		PageToken: string(req.GetNextPageToken()),
	}, nil
}

// listNamespacesResponse folds a GetNamespaces response into the ListNamespaces
// response the caller is waiting on, applying the request's namespace filter,
// which the Cloud request had nowhere to carry.
func listNamespacesResponse(
	req *workflowservice.ListNamespacesRequest,
	up *cloudservice.GetNamespacesResponse,
	reply *workflowservice.ListNamespacesResponse,
) error {
	includeDeleted := req.GetNamespaceFilter().GetIncludeDeleted()

	namespaces := make([]*workflowservice.DescribeNamespaceResponse, 0, len(up.GetNamespaces()))
	for _, ns := range up.GetNamespaces() {
		state := namespaceState(ns.GetState())
		if state == enumspb.NAMESPACE_STATE_DELETED && !includeDeleted {
			continue
		}

		namespaces = append(namespaces, describeNamespace(ns, state))
	}

	reply.Namespaces = namespaces
	reply.NextPageToken = []byte(up.GetNextPageToken())

	return nil
}

// describeNamespace converts one Cloud namespace into the DescribeNamespace
// response ListNamespaces returns per namespace, with state already mapped by
// the caller so the deleted filter and this conversion agree on it.
//
// Only fields Cloud actually reports are set. Cloud has no namespace UUID,
// description, or owner email to give, and it describes replication as regional
// replicas rather than as Temporal clusters, so IsGlobalNamespace is derived
// from how many replicas there are while ReplicationConfig is left unset rather
// than filled with regions a client would read as cluster names.
// FailoverVersion and FailoverHistory have no Cloud equivalent at all.
func describeNamespace(
	ns *cloudnamespace.Namespace,
	state enumspb.NamespaceState,
) *workflowservice.DescribeNamespaceResponse {
	spec := ns.GetSpec()

	out := &workflowservice.DescribeNamespaceResponse{
		NamespaceInfo: &namespacepb.NamespaceInfo{
			// The Cloud namespace identifier is the fully qualified name
			// ("<namespace>.<account>"), which is the name a client addresses the
			// namespace by, so it is the name rather than the id.
			Name:  ns.GetNamespace(),
			State: state,
			Data:  ns.GetTags(),
		},
		IsGlobalNamespace: len(spec.GetReplicas()) > 1,
	}

	// Cloud reports retention in whole days, and zero means it did not report one
	// rather than "retain nothing"; leaving the config unset says that, where a
	// zero duration would claim a retention Cloud never stated.
	if days := spec.GetRetentionDays(); days > 0 {
		out.Config = &namespacepb.NamespaceConfig{
			WorkflowExecutionRetentionTtl: durationpb.New(time.Duration(days) * hoursPerDay),
		}
	}

	return out
}

// namespaceState maps the lifecycle state Cloud reports for a resource onto the
// namespace state a WorkflowService client expects. Cloud draws finer
// distinctions than the three states Temporal has: every state in which the
// namespace exists and is being worked on reads as registered, every stage of
// removal reads as deleted, and a namespace withdrawn from use without being
// removed reads as deprecated. An unrecognized state stays unspecified rather
// than being guessed at.
func namespaceState(state resource.ResourceState) enumspb.NamespaceState {
	switch state {
	case resource.ResourceState_RESOURCE_STATE_ACTIVATING,
		resource.ResourceState_RESOURCE_STATE_ACTIVATION_FAILED,
		resource.ResourceState_RESOURCE_STATE_ACTIVE,
		resource.ResourceState_RESOURCE_STATE_UPDATING,
		resource.ResourceState_RESOURCE_STATE_UPDATE_FAILED:
		return enumspb.NAMESPACE_STATE_REGISTERED

	case resource.ResourceState_RESOURCE_STATE_DELETING,
		resource.ResourceState_RESOURCE_STATE_DELETE_FAILED,
		resource.ResourceState_RESOURCE_STATE_DELETED:
		return enumspb.NAMESPACE_STATE_DELETED

	case resource.ResourceState_RESOURCE_STATE_SUSPENDED,
		resource.ResourceState_RESOURCE_STATE_EXPIRED:
		return enumspb.NAMESPACE_STATE_DEPRECATED

	default:
		return enumspb.NAMESPACE_STATE_UNSPECIFIED
	}
}
