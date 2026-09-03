package translation

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	namespacepb "go.temporal.io/api/namespace/v1"
	workflowservice "go.temporal.io/api/workflowservice/v1"
	cloudservice "go.temporal.io/cloud-sdk/api/cloudservice/v1"
	cloudnamespace "go.temporal.io/cloud-sdk/api/namespace/v1"
	"go.temporal.io/cloud-sdk/api/resource/v1"
)

func TestListNamespacesRequestCarriesPagination(t *testing.T) {
	t.Parallel()

	got, err := listNamespacesRequest(&workflowservice.ListNamespacesRequest{
		PageSize:      50,
		NextPageToken: []byte("token"),
	})
	require.NoError(t, err)
	require.Equal(t, int32(50), got.GetPageSize())
	require.Equal(t, "token", got.GetPageToken())
}

func TestListNamespacesRequestClampsPageSize(t *testing.T) {
	t.Parallel()

	got, err := listNamespacesRequest(&workflowservice.ListNamespacesRequest{PageSize: cloudPageLimit + 1})
	require.NoError(t, err)
	require.Equal(t, int32(cloudPageLimit), got.GetPageSize(), "Cloud rejects a larger page outright")
}

func TestListNamespacesResponseMapsNamespaceFields(t *testing.T) {
	t.Parallel()

	up := &cloudservice.GetNamespacesResponse{
		Namespaces: []*cloudnamespace.Namespace{{
			Namespace: "payments.a1b2c",
			State:     resource.ResourceState_RESOURCE_STATE_ACTIVE,
			Tags:      map[string]string{"team": "payments"},
			Spec: &cloudnamespace.NamespaceSpec{
				Name:          "payments",
				RetentionDays: 30,
				Replicas: []*cloudnamespace.ReplicaSpec{
					{Region: "aws-us-east-1"},
					{Region: "aws-us-west-2"},
				},
			},
		}},
		NextPageToken: "next",
	}

	reply := &workflowservice.ListNamespacesResponse{}
	require.NoError(t, listNamespacesResponse(&workflowservice.ListNamespacesRequest{}, up, reply))

	require.Len(t, reply.GetNamespaces(), 1)
	ns := reply.GetNamespaces()[0]

	require.Equal(t, "payments.a1b2c", ns.GetNamespaceInfo().GetName(), "the fully qualified name is what clients address")
	require.Equal(t, enumspb.NAMESPACE_STATE_REGISTERED, ns.GetNamespaceInfo().GetState())
	require.Equal(t, map[string]string{"team": "payments"}, ns.GetNamespaceInfo().GetData())
	require.Equal(t, 30*24*time.Hour, ns.GetConfig().GetWorkflowExecutionRetentionTtl().AsDuration())
	require.True(t, ns.GetIsGlobalNamespace(), "replicated across more than one region")
	require.Nil(t, ns.GetReplicationConfig(), "Cloud reports regional replicas, not Temporal clusters")

	require.Equal(t, []byte("next"), reply.GetNextPageToken())
}

func TestListNamespacesResponseOmitsUnreportedRetention(t *testing.T) {
	t.Parallel()

	up := &cloudservice.GetNamespacesResponse{
		Namespaces: []*cloudnamespace.Namespace{{
			Namespace: "payments.a1b2c",
			State:     resource.ResourceState_RESOURCE_STATE_ACTIVE,
			Spec: &cloudnamespace.NamespaceSpec{
				Name:     "payments",
				Replicas: []*cloudnamespace.ReplicaSpec{{Region: "aws-us-east-1"}},
			},
		}},
	}

	reply := &workflowservice.ListNamespacesResponse{}
	require.NoError(t, listNamespacesResponse(&workflowservice.ListNamespacesRequest{}, up, reply))

	ns := reply.GetNamespaces()[0]
	require.Nil(t, ns.GetConfig(), "no retention reported is not a zero retention")
	require.False(t, ns.GetIsGlobalNamespace())
	require.Empty(t, reply.GetNextPageToken())
}

func TestListNamespacesResponseAppliesTheDeletedFilter(t *testing.T) {
	t.Parallel()

	up := &cloudservice.GetNamespacesResponse{
		Namespaces: []*cloudnamespace.Namespace{
			{Namespace: "live.a1b2c", State: resource.ResourceState_RESOURCE_STATE_ACTIVE},
			{Namespace: "gone.a1b2c", State: resource.ResourceState_RESOURCE_STATE_DELETED},
		},
	}

	// The Cloud request has nowhere to carry the filter, so it is honoured here.
	excluded := &workflowservice.ListNamespacesResponse{}
	require.NoError(t, listNamespacesResponse(&workflowservice.ListNamespacesRequest{}, up, excluded))
	require.Len(t, excluded.GetNamespaces(), 1)
	require.Equal(t, "live.a1b2c", excluded.GetNamespaces()[0].GetNamespaceInfo().GetName())

	included := &workflowservice.ListNamespacesResponse{}
	require.NoError(t, listNamespacesResponse(&workflowservice.ListNamespacesRequest{
		NamespaceFilter: &namespacepb.NamespaceFilter{IncludeDeleted: true},
	}, up, included))
	require.Len(t, included.GetNamespaces(), 2)
}

func TestListNamespacesResponseHandlesAnEmptyPage(t *testing.T) {
	t.Parallel()

	reply := &workflowservice.ListNamespacesResponse{}
	require.NoError(t, listNamespacesResponse(
		&workflowservice.ListNamespacesRequest{},
		&cloudservice.GetNamespacesResponse{},
		reply,
	))
	require.Empty(t, reply.GetNamespaces())
	require.Empty(t, reply.GetNextPageToken())
}

func TestNamespaceState(t *testing.T) {
	t.Parallel()

	cases := map[resource.ResourceState]enumspb.NamespaceState{
		resource.ResourceState_RESOURCE_STATE_ACTIVATING:        enumspb.NAMESPACE_STATE_REGISTERED,
		resource.ResourceState_RESOURCE_STATE_ACTIVATION_FAILED: enumspb.NAMESPACE_STATE_REGISTERED,
		resource.ResourceState_RESOURCE_STATE_ACTIVE:            enumspb.NAMESPACE_STATE_REGISTERED,
		resource.ResourceState_RESOURCE_STATE_UPDATING:          enumspb.NAMESPACE_STATE_REGISTERED,
		resource.ResourceState_RESOURCE_STATE_UPDATE_FAILED:     enumspb.NAMESPACE_STATE_REGISTERED,
		resource.ResourceState_RESOURCE_STATE_DELETING:          enumspb.NAMESPACE_STATE_DELETED,
		resource.ResourceState_RESOURCE_STATE_DELETE_FAILED:     enumspb.NAMESPACE_STATE_DELETED,
		resource.ResourceState_RESOURCE_STATE_DELETED:           enumspb.NAMESPACE_STATE_DELETED,
		resource.ResourceState_RESOURCE_STATE_SUSPENDED:         enumspb.NAMESPACE_STATE_DEPRECATED,
		resource.ResourceState_RESOURCE_STATE_EXPIRED:           enumspb.NAMESPACE_STATE_DEPRECATED,
		resource.ResourceState_RESOURCE_STATE_UNSPECIFIED:       enumspb.NAMESPACE_STATE_UNSPECIFIED,
		resource.ResourceState(999):                             enumspb.NAMESPACE_STATE_UNSPECIFIED,
	}

	for state, want := range cases {
		require.Equal(t, want, namespaceState(state), "state %v", state)
	}
}
