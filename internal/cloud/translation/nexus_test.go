package translation

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	nexuspb "go.temporal.io/api/nexus/v1"
	operatorservice "go.temporal.io/api/operatorservice/v1"
	cloudservice "go.temporal.io/cloud-sdk/api/cloudservice/v1"
	cloudnexus "go.temporal.io/cloud-sdk/api/nexus/v1"
	"go.temporal.io/cloud-sdk/api/resource/v1"
	"go.temporal.io/cloud-sdk/cloudclient"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// nexusLister is a fake Cloud control plane serving the two Nexus endpoint reads.
// It records the version header each call carried.
type nexusLister struct {
	cloudservice.UnimplementedCloudServiceServer

	lis net.Listener

	mu       sync.Mutex
	versions []string
}

func TestListNexusEndpointsRequest(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		req      *operatorservice.ListNexusEndpointsRequest
		wantSize int32
	}{
		{
			name:     "carries pagination and the name filter",
			req:      &operatorservice.ListNexusEndpointsRequest{PageSize: 50, NextPageToken: []byte("token"), Name: "orders"},
			wantSize: 50,
		},
		{
			name:     "clamps the page size Cloud would reject",
			req:      &operatorservice.ListNexusEndpointsRequest{PageSize: cloudPageLimit + 1, NextPageToken: []byte("token"), Name: "orders"},
			wantSize: cloudPageLimit,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := listNexusEndpointsRequest(tt.req)
			require.NoError(t, err)
			require.Equal(t, tt.wantSize, got.GetPageSize())
			require.Equal(t, "token", got.GetPageToken())
			require.Equal(t, "orders", got.GetName())
		})
	}
}

func TestListNexusEndpointsResponse(t *testing.T) {
	t.Parallel()

	created := timestamppb.New(time.Unix(1_700_000_000, 0))
	modified := timestamppb.New(time.Unix(1_700_000_100, 0))
	description := &commonpb.Payload{Data: []byte(`"routes orders"`)}

	up := &cloudservice.GetNexusEndpointsResponse{
		Endpoints: []*cloudnexus.Endpoint{
			{
				Id:               "ep-1",
				ResourceVersion:  "opaque-etag",
				State:            resource.ResourceState_RESOURCE_STATE_ACTIVE,
				CreatedTime:      created,
				LastModifiedTime: modified,
				Spec: &cloudnexus.EndpointSpec{
					Name:        "orders",
					Description: description,
					TargetSpec: &cloudnexus.EndpointTargetSpec{
						Variant: &cloudnexus.EndpointTargetSpec_WorkerTargetSpec{
							WorkerTargetSpec: &cloudnexus.WorkerTargetSpec{NamespaceId: "orders.a1b2c", TaskQueue: "orders-tq"},
						},
					},
				},
			},
			{Id: "ep-gone", State: resource.ResourceState_RESOURCE_STATE_DELETED},
		},
		NextPageToken: "next",
	}

	reply := &operatorservice.ListNexusEndpointsResponse{}
	require.NoError(t, listNexusEndpointsResponse(&operatorservice.ListNexusEndpointsRequest{}, up, reply))

	// Version stays zero, since Cloud's resource version is an opaque string, and
	// UrlPrefix stays empty rather than naming a path Cloud does not serve.
	want := &operatorservice.ListNexusEndpointsResponse{
		Endpoints: []*nexuspb.Endpoint{{
			Id:               "ep-1",
			CreatedTime:      created,
			LastModifiedTime: modified,
			Spec: &nexuspb.EndpointSpec{
				Name:        "orders",
				Description: description,
				Target: &nexuspb.EndpointTarget{
					Variant: &nexuspb.EndpointTarget_Worker_{
						Worker: &nexuspb.EndpointTarget_Worker{Namespace: "orders.a1b2c", TaskQueue: "orders-tq"},
					},
				},
			},
		}},
		NextPageToken: []byte("next"),
	}
	require.True(t, proto.Equal(want, reply), "got %v", reply)
}

func TestGetNexusEndpoint(t *testing.T) {
	t.Parallel()

	req := &operatorservice.GetNexusEndpointRequest{Id: "ep-1"}

	upReq, err := getNexusEndpointRequest(req)
	require.NoError(t, err)
	require.Equal(t, "ep-1", upReq.GetEndpointId())

	up := &cloudservice.GetNexusEndpointResponse{Endpoint: &cloudnexus.Endpoint{
		Id:   "ep-1",
		Spec: &cloudnexus.EndpointSpec{Name: "orders"},
	}}

	reply := &operatorservice.GetNexusEndpointResponse{}
	require.NoError(t, getNexusEndpointResponse(req, up, reply))
	require.Equal(t, "ep-1", reply.GetEndpoint().GetId())
	require.Equal(t, "orders", reply.GetEndpoint().GetSpec().GetName())
}

func TestNexusEndpointsOverARealCloudServiceConnection(t *testing.T) {
	t.Parallel()

	// A real CloudService server is the only thing that rejects a misspelled
	// upstream method, which a Lookup comparing against the same constant cannot.
	up := newNexusLister(t)

	r, err := Default()
	require.NoError(t, err)

	cc, err := grpc.NewClient(
		up.lis.Addr().String(),
		append([]grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}, DialOptions(r)...)...,
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = cc.Close() })

	list := &operatorservice.ListNexusEndpointsResponse{}
	require.NoError(t, cc.Invoke(t.Context(), listNexusEndpointsMethod, &operatorservice.ListNexusEndpointsRequest{}, list))
	require.Len(t, list.GetEndpoints(), 1)
	require.Equal(t, "ep-1", list.GetEndpoints()[0].GetId())

	get := &operatorservice.GetNexusEndpointResponse{}
	require.NoError(t, cc.Invoke(t.Context(), getNexusEndpointMethod, &operatorservice.GetNexusEndpointRequest{Id: "ep-1"}, get))
	require.Equal(t, "ep-1", get.GetEndpoint().GetId())

	require.Equal(t,
		[]string{cloudclient.DefaultAPIVersion(), cloudclient.DefaultAPIVersion()},
		up.recorded(),
		"Cloud rejects both calls outright without the API version",
	)
}

// newNexusLister starts the fake on a loopback port and stops it when the test
// ends.
func newNexusLister(t *testing.T) *nexusLister {
	t.Helper()

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	up := &nexusLister{lis: lis}

	svr := grpc.NewServer()
	cloudservice.RegisterCloudServiceServer(svr, up)
	go func() { _ = svr.Serve(lis) }()
	t.Cleanup(svr.Stop)

	return up
}

func (l *nexusLister) GetNexusEndpoints(
	ctx context.Context, _ *cloudservice.GetNexusEndpointsRequest,
) (*cloudservice.GetNexusEndpointsResponse, error) {
	l.record(ctx)
	return &cloudservice.GetNexusEndpointsResponse{Endpoints: []*cloudnexus.Endpoint{{Id: "ep-1"}}}, nil
}

func (l *nexusLister) GetNexusEndpoint(
	ctx context.Context, req *cloudservice.GetNexusEndpointRequest,
) (*cloudservice.GetNexusEndpointResponse, error) {
	l.record(ctx)
	return &cloudservice.GetNexusEndpointResponse{Endpoint: &cloudnexus.Endpoint{Id: req.GetEndpointId()}}, nil
}

func (l *nexusLister) record(ctx context.Context) {
	md, _ := metadata.FromIncomingContext(ctx)

	l.mu.Lock()
	defer l.mu.Unlock()

	l.versions = append(l.versions, md.Get(cloudclient.TemporalCloudAPIVersionHeader())...)
}

func (l *nexusLister) recorded() []string {
	l.mu.Lock()
	defer l.mu.Unlock()

	return append([]string(nil), l.versions...)
}
