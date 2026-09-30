package translation

import (
	"context"
	"net"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	operatorservice "go.temporal.io/api/operatorservice/v1"
	cloudservice "go.temporal.io/cloud-sdk/api/cloudservice/v1"
	cloudnamespace "go.temporal.io/cloud-sdk/api/namespace/v1"
	"go.temporal.io/cloud-sdk/cloudclient"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"
)

// namespaceGetter is a fake Cloud control plane serving GetNamespace. It records
// the request and the version header it was called with.
type namespaceGetter struct {
	cloudservice.UnimplementedCloudServiceServer

	lis net.Listener

	mu      sync.Mutex
	got     *cloudservice.GetNamespaceRequest
	version []string
}

func TestListSearchAttributesRequestCarriesTheNamespace(t *testing.T) {
	t.Parallel()

	// The namespace translator runs outside this one, so the name here is already
	// the remote form GetNamespace addresses.
	got, err := listSearchAttributesRequest(&operatorservice.ListSearchAttributesRequest{Namespace: "payments.a1b2c"})
	require.NoError(t, err)
	require.Equal(t, "payments.a1b2c", got.GetNamespace())
}

func TestListSearchAttributesResponse(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		attrs map[string]cloudnamespace.NamespaceSpec_SearchAttributeType
		want  map[string]enumspb.IndexedValueType
	}{
		{
			name: "maps every type Cloud reports",
			attrs: map[string]cloudnamespace.NamespaceSpec_SearchAttributeType{
				"Text":        cloudnamespace.NamespaceSpec_SEARCH_ATTRIBUTE_TYPE_TEXT,
				"Keyword":     cloudnamespace.NamespaceSpec_SEARCH_ATTRIBUTE_TYPE_KEYWORD,
				"Int":         cloudnamespace.NamespaceSpec_SEARCH_ATTRIBUTE_TYPE_INT,
				"Double":      cloudnamespace.NamespaceSpec_SEARCH_ATTRIBUTE_TYPE_DOUBLE,
				"Bool":        cloudnamespace.NamespaceSpec_SEARCH_ATTRIBUTE_TYPE_BOOL,
				"Datetime":    cloudnamespace.NamespaceSpec_SEARCH_ATTRIBUTE_TYPE_DATETIME,
				"KeywordList": cloudnamespace.NamespaceSpec_SEARCH_ATTRIBUTE_TYPE_KEYWORD_LIST,
			},
			want: map[string]enumspb.IndexedValueType{
				"Text":        enumspb.INDEXED_VALUE_TYPE_TEXT,
				"Keyword":     enumspb.INDEXED_VALUE_TYPE_KEYWORD,
				"Int":         enumspb.INDEXED_VALUE_TYPE_INT,
				"Double":      enumspb.INDEXED_VALUE_TYPE_DOUBLE,
				"Bool":        enumspb.INDEXED_VALUE_TYPE_BOOL,
				"Datetime":    enumspb.INDEXED_VALUE_TYPE_DATETIME,
				"KeywordList": enumspb.INDEXED_VALUE_TYPE_KEYWORD_LIST,
			},
		},
		{
			name:  "leaves a type it does not recognize unspecified",
			attrs: map[string]cloudnamespace.NamespaceSpec_SearchAttributeType{"Future": 99},
			want:  map[string]enumspb.IndexedValueType{"Future": enumspb.INDEXED_VALUE_TYPE_UNSPECIFIED},
		},
		{
			name: "reports nothing for a namespace with no custom attributes",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			up := &cloudservice.GetNamespaceResponse{Namespace: &cloudnamespace.Namespace{
				Spec: &cloudnamespace.NamespaceSpec{SearchAttributes: tt.attrs},
			}}

			// Pre-filled so the test shows stale values are cleared: Cloud reports no
			// system attributes or storage schema, so none may survive.
			reply := &operatorservice.ListSearchAttributesResponse{
				SystemAttributes: map[string]enumspb.IndexedValueType{"WorkflowType": enumspb.INDEXED_VALUE_TYPE_KEYWORD},
				StorageSchema:    map[string]string{"stale": "stale"},
			}
			require.NoError(t, listSearchAttributesResponse(&operatorservice.ListSearchAttributesRequest{}, up, reply))

			want := &operatorservice.ListSearchAttributesResponse{CustomAttributes: tt.want}
			require.True(t, proto.Equal(want, reply), "got %v", reply)
		})
	}
}

func TestListSearchAttributesOverARealCloudServiceConnection(t *testing.T) {
	t.Parallel()

	// A real CloudService server is the only thing that rejects a misspelled
	// upstream method, which a Lookup comparing against the same constant cannot.
	up := newNamespaceGetter(t)

	r, err := Default()
	require.NoError(t, err)

	cc, err := grpc.NewClient(
		up.lis.Addr().String(),
		append([]grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}, DialOptions(r)...)...,
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = cc.Close() })

	reply := &operatorservice.ListSearchAttributesResponse{}
	err = cc.Invoke(t.Context(), listSearchAttributesMethod,
		&operatorservice.ListSearchAttributesRequest{Namespace: "payments.a1b2c"}, reply)
	require.NoError(t, err)

	require.Equal(t,
		map[string]enumspb.IndexedValueType{"OrderId": enumspb.INDEXED_VALUE_TYPE_KEYWORD},
		reply.GetCustomAttributes(),
	)

	got, version := up.recorded()
	require.Equal(t, "payments.a1b2c", got.GetNamespace())
	require.Equal(t, []string{cloudclient.DefaultAPIVersion()}, version,
		"Cloud rejects GetNamespace outright without the API version")
}

// newNamespaceGetter starts the fake on a loopback port and stops it when the
// test ends.
func newNamespaceGetter(t *testing.T) *namespaceGetter {
	t.Helper()

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	up := &namespaceGetter{lis: lis}

	svr := grpc.NewServer()
	cloudservice.RegisterCloudServiceServer(svr, up)
	go func() { _ = svr.Serve(lis) }()
	t.Cleanup(svr.Stop)

	return up
}

func (g *namespaceGetter) GetNamespace(
	ctx context.Context, req *cloudservice.GetNamespaceRequest,
) (*cloudservice.GetNamespaceResponse, error) {
	md, _ := metadata.FromIncomingContext(ctx)

	g.mu.Lock()
	g.got = req
	g.version = md.Get(cloudclient.TemporalCloudAPIVersionHeader())
	g.mu.Unlock()

	return &cloudservice.GetNamespaceResponse{Namespace: &cloudnamespace.Namespace{
		Namespace: req.GetNamespace(),
		Spec: &cloudnamespace.NamespaceSpec{SearchAttributes: map[string]cloudnamespace.NamespaceSpec_SearchAttributeType{
			"OrderId": cloudnamespace.NamespaceSpec_SEARCH_ATTRIBUTE_TYPE_KEYWORD,
		}},
	}}, nil
}

func (g *namespaceGetter) recorded() (*cloudservice.GetNamespaceRequest, []string) {
	g.mu.Lock()
	defer g.mu.Unlock()

	return g.got, append([]string(nil), g.version...)
}
