package translation

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	namespacepb "go.temporal.io/api/namespace/v1"
	workflowservice "go.temporal.io/api/workflowservice/v1"
	cloudservice "go.temporal.io/cloud-sdk/api/cloudservice/v1"
	cloudnamespace "go.temporal.io/cloud-sdk/api/namespace/v1"
	"go.temporal.io/cloud-sdk/api/resource/v1"
	"go.temporal.io/cloud-sdk/cloudclient"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
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

// namespaceLister is a fake Temporal Cloud control plane. It serves CloudService
// and nothing else, the way saas-api does.
type namespaceLister struct {
	cloudservice.UnimplementedCloudServiceServer

	lis net.Listener

	// mu guards the recorded state, which the serving goroutine writes while the
	// test reads.
	mu   sync.Mutex
	got  *cloudservice.GetNamespacesRequest
	md   metadata.MD
	page *cloudservice.GetNamespacesResponse
}

func TestDefaultRegistryIsSharedAndTranslatesListNamespaces(t *testing.T) {
	t.Parallel()

	r, err := Default()
	require.NoError(t, err)

	again, err := Default()
	require.NoError(t, err)
	require.Same(t, r, again, "the shipped registry is built once")

	tr, ok := r.Lookup(listNamespacesMethod)
	require.True(t, ok)
	require.Equal(t, getNamespacesMethod, tr.To())
}

func TestListNamespacesPinsTheCloudAPIVersion(t *testing.T) {
	t.Parallel()

	r, err := Default()
	require.NoError(t, err)

	tr, ok := r.Lookup(listNamespacesMethod)
	require.True(t, ok)

	md, mdOK := metadata.FromOutgoingContext(tr.stamp(t.Context()))
	require.True(t, mdOK)
	require.Equal(t,
		[]string{cloudclient.DefaultAPIVersion()},
		md.Get(cloudclient.TemporalCloudAPIVersionHeader()),
		"GetNamespaces fails with InvalidArgument without it",
	)
}

func TestListNamespacesOverARealCloudServiceConnection(t *testing.T) {
	t.Parallel()

	upstream := newNamespaceLister(t, &cloudservice.GetNamespacesResponse{
		Namespaces:    []*cloudnamespace.Namespace{{Namespace: "payments.a1b2c", State: resource.ResourceState_RESOURCE_STATE_ACTIVE}},
		NextPageToken: "next",
	})

	r, err := Default()
	require.NoError(t, err)

	dialOpts := append(
		[]grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())},
		DialOptions(r)...,
	)
	cc, err := grpc.NewClient(upstream.lis.Addr().String(), dialOpts...)
	require.NoError(t, err)
	t.Cleanup(func() { _ = cc.Close() })

	// Invoke the WorkflowService method by name, the way the reflective forwarder
	// does: the interceptor is what turns it into a CloudService call.
	reply := &workflowservice.ListNamespacesResponse{}
	err = cc.Invoke(t.Context(), listNamespacesMethod, &workflowservice.ListNamespacesRequest{PageSize: 3}, reply)
	require.NoError(t, err)

	got, md := upstream.recorded()
	require.Equal(t, int32(3), got.GetPageSize())
	require.Equal(t,
		[]string{cloudclient.DefaultAPIVersion()},
		md.Get(cloudclient.TemporalCloudAPIVersionHeader()),
		"Cloud rejects GetNamespaces outright without the API version",
	)

	require.Len(t, reply.GetNamespaces(), 1)
	require.Equal(t, "payments.a1b2c", reply.GetNamespaces()[0].GetNamespaceInfo().GetName())
	require.Equal(t, []byte("next"), reply.GetNextPageToken())
}

// newNamespaceLister starts the fake control plane on a loopback port and stops
// it when the test ends.
func newNamespaceLister(t *testing.T, page *cloudservice.GetNamespacesResponse) *namespaceLister {
	t.Helper()

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	up := &namespaceLister{lis: lis, page: page}

	svr := grpc.NewServer()
	cloudservice.RegisterCloudServiceServer(svr, up)
	go func() { _ = svr.Serve(lis) }()
	t.Cleanup(svr.Stop)

	return up
}

func (l *namespaceLister) GetNamespaces(
	ctx context.Context, req *cloudservice.GetNamespacesRequest,
) (*cloudservice.GetNamespacesResponse, error) {
	md, _ := metadata.FromIncomingContext(ctx)

	l.mu.Lock()
	defer l.mu.Unlock()

	l.got = req
	l.md = md

	return l.page, nil
}

func (l *namespaceLister) recorded() (*cloudservice.GetNamespacesRequest, metadata.MD) {
	l.mu.Lock()
	defer l.mu.Unlock()

	return l.got, l.md.Copy()
}
