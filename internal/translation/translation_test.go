package translation

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	workflowservice "go.temporal.io/api/workflowservice/v1"
	cloudservice "go.temporal.io/cloud-sdk/api/cloudservice/v1"
	"go.temporal.io/cloud-sdk/cloudclient"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"
)

func TestNewRegistryCanonicalizesMethods(t *testing.T) {
	t.Parallel()

	// Written without the leading slash gRPC supplies, so the registry has to
	// normalize both ends before it can match anything.
	r, err := NewRegistry(Adapt(
		"pkg.Service/From",
		"pkg.Other/To",
		okRequest,
		okResponse,
	))
	require.NoError(t, err)

	got, ok := r.Lookup("/pkg.Service/From")
	require.True(t, ok)
	require.Equal(t, "/pkg.Service/From", got.From())
	require.Equal(t, "/pkg.Other/To", got.To())
	require.Equal(t, []string{"/pkg.Service/From"}, r.Methods())
}

func TestNewRegistryRejectsBadMappings(t *testing.T) {
	t.Parallel()

	cases := map[string]struct {
		in   []*Translation
		want string
	}{
		"nil entry": {
			in:   []*Translation{nil},
			want: "nil translation at index 0",
		},
		"malformed from": {
			in:   []*Translation{Adapt("NotAMethod", "/pkg.Other/To", okRequest, okResponse)},
			want: `"NotAMethod" is not a gRPC full method`,
		},
		"malformed to": {
			in:   []*Translation{Adapt("/pkg.Service/From", "NotAMethod", okRequest, okResponse)},
			want: `"NotAMethod" is not a gRPC full method`,
		},
		"self mapping": {
			in:   []*Translation{Adapt("/pkg.Service/From", "/pkg.Service/From", okRequest, okResponse)},
			want: "translates onto itself",
		},
		"duplicate from": {
			in: []*Translation{
				Adapt("/pkg.Service/From", "/pkg.Other/To", okRequest, okResponse),
				Adapt("/pkg.Service/From", "/pkg.Third/To", okRequest, okResponse),
			},
			want: "is translated twice",
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			r, err := NewRegistry(tc.in...)
			require.ErrorContains(t, err, tc.want)
			require.Nil(t, r)
		})
	}
}

func TestRegistryLookupMisses(t *testing.T) {
	t.Parallel()

	var nilRegistry *Registry
	_, ok := nilRegistry.Lookup("/pkg.Service/From")
	require.False(t, ok, "a nil registry translates nothing")
	require.Nil(t, nilRegistry.Methods())

	empty, err := NewRegistry()
	require.NoError(t, err)
	_, ok = empty.Lookup("/pkg.Service/From")
	require.False(t, ok)
	require.Empty(t, empty.Methods())
}

func TestAdaptRejectsMismatchedMessages(t *testing.T) {
	t.Parallel()

	tr := Adapt("/pkg.Service/From", "/pkg.Other/To", okRequest, okResponse)

	_, err := tr.request(&workflowservice.DescribeNamespaceRequest{})
	require.ErrorContains(t, err, "wanted a temporal.api.workflowservice.v1.ListNamespacesRequest request")

	req := &workflowservice.ListNamespacesRequest{}
	err = tr.response(req, &workflowservice.ListNamespacesResponse{}, &workflowservice.ListNamespacesResponse{})
	require.ErrorContains(t, err, "wanted a temporal.api.cloud.cloudservice.v1.GetNamespacesResponse reply")

	err = tr.response(req, &cloudservice.GetNamespacesResponse{}, &workflowservice.DescribeNamespaceResponse{})
	require.ErrorContains(t, err, "wanted a temporal.api.workflowservice.v1.ListNamespacesResponse reply")
}

func TestAdaptPropagatesConversionErrors(t *testing.T) {
	t.Parallel()

	boom := errors.New("boom")
	tr := Adapt(
		"/pkg.Service/From",
		"/pkg.Other/To",
		func(*workflowservice.ListNamespacesRequest) (*cloudservice.GetNamespacesRequest, error) {
			return nil, boom
		},
		okResponse,
	)

	_, err := tr.request(&workflowservice.ListNamespacesRequest{})
	require.ErrorIs(t, err, boom)
}

func TestTranslationAllocatesUpstreamReply(t *testing.T) {
	t.Parallel()

	tr := Adapt("/pkg.Service/From", "/pkg.Other/To", okRequest, okResponse)

	reply := tr.reply()
	require.IsType(t, &cloudservice.GetNamespacesResponse{}, reply)
	require.NotSame(t, reply, tr.reply(), "each call allocates its own reply")
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

func okRequest(*workflowservice.ListNamespacesRequest) (*cloudservice.GetNamespacesRequest, error) {
	return &cloudservice.GetNamespacesRequest{}, nil
}

func okResponse(
	_ *workflowservice.ListNamespacesRequest,
	_ *cloudservice.GetNamespacesResponse,
	_ *workflowservice.ListNamespacesResponse,
) error {
	return nil
}

// mustProto fails the test unless m is the message the caller expected.
func mustProto[T proto.Message](t *testing.T, m any) T {
	t.Helper()

	out, ok := m.(T)
	require.True(t, ok, "got %T", m)
	return out
}

func TestWithHeaderStampsOnlyTheSubstitutedCall(t *testing.T) {
	t.Parallel()

	tr := Adapt("/pkg.Service/From", "/pkg.Other/To", okRequest, okResponse).
		WithHeader("x-api-version", "v1")

	md, ok := metadata.FromOutgoingContext(tr.stamp(t.Context()))
	require.True(t, ok)
	require.Equal(t, []string{"v1"}, md.Get("x-api-version"))

	// A translation that declares no header leaves the context alone.
	plain := Adapt("/pkg.Service/From", "/pkg.Other/To", okRequest, okResponse)
	ctx := t.Context()
	require.Equal(t, ctx, plain.stamp(ctx))
}

func TestWithHeaderReplacesAnInboundValue(t *testing.T) {
	t.Parallel()

	tr := Adapt("/pkg.Service/From", "/pkg.Other/To", okRequest, okResponse).
		WithHeader("x-api-version", "v2")

	// The caller's own value was forwarded onto the outgoing context; the
	// translation pins the version its conversions were written against, and must
	// leave exactly one value rather than appending a second.
	ctx := metadata.NewOutgoingContext(t.Context(), metadata.Pairs("x-api-version", "v1"))

	md, ok := metadata.FromOutgoingContext(tr.stamp(ctx))
	require.True(t, ok)
	require.Equal(t, []string{"v2"}, md.Get("x-api-version"))
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
