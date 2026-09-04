package translation

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	namespacepb "go.temporal.io/api/namespace/v1"
	workflowservice "go.temporal.io/api/workflowservice/v1"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"
)

// The mechanism is generic, so it is exercised with a made-up pairing rather than
// with a translation the proxy ships: DescribeNamespace onto GetSystemInfo. The
// types are unrelated in every way that matters here, which is the point - a
// test that used a real mapping's types could pass for the wrong reason.
const (
	fromMethod = "/temporal.api.workflowservice.v1.WorkflowService/DescribeNamespace"
	toMethod   = "/temporal.api.workflowservice.v1.WorkflowService/GetSystemInfo"
)

func TestNewRegistryCanonicalizesMethods(t *testing.T) {
	t.Parallel()

	// Written without the leading slash gRPC supplies, so the registry has to
	// normalize both ends before it can match anything.
	r, err := NewRegistry(Adapt("pkg.Service/From", "pkg.Other/To", okRequest, okResponse))
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
			in:   []*Translation{Adapt("NotAMethod", toMethod, okRequest, okResponse)},
			want: `"NotAMethod" is not a gRPC full method`,
		},
		"malformed to": {
			in:   []*Translation{Adapt(fromMethod, "NotAMethod", okRequest, okResponse)},
			want: `"NotAMethod" is not a gRPC full method`,
		},
		"self mapping": {
			in:   []*Translation{Adapt(fromMethod, fromMethod, okRequest, okResponse)},
			want: "translates onto itself",
		},
		"duplicate from": {
			in: []*Translation{
				Adapt(fromMethod, toMethod, okRequest, okResponse),
				Adapt(fromMethod, "/pkg.Third/To", okRequest, okResponse),
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
	_, ok := nilRegistry.Lookup(fromMethod)
	require.False(t, ok, "a nil registry translates nothing")
	require.Nil(t, nilRegistry.Methods())

	empty, err := NewRegistry()
	require.NoError(t, err)
	_, ok = empty.Lookup(fromMethod)
	require.False(t, ok)
	require.Empty(t, empty.Methods())
}

func TestAdaptRejectsMismatchedMessages(t *testing.T) {
	t.Parallel()

	tr := Adapt(fromMethod, toMethod, okRequest, okResponse)

	_, err := tr.request(&workflowservice.ListNamespacesRequest{})
	require.ErrorContains(t, err, "wanted a temporal.api.workflowservice.v1.DescribeNamespaceRequest request")

	req := &workflowservice.DescribeNamespaceRequest{}
	err = tr.response(req, &workflowservice.ListNamespacesResponse{}, &workflowservice.DescribeNamespaceResponse{})
	require.ErrorContains(t, err, "wanted a temporal.api.workflowservice.v1.GetSystemInfoResponse reply")

	err = tr.response(req, &workflowservice.GetSystemInfoResponse{}, &workflowservice.ListNamespacesResponse{})
	require.ErrorContains(t, err, "wanted a temporal.api.workflowservice.v1.DescribeNamespaceResponse reply")
}

func TestAdaptPropagatesConversionErrors(t *testing.T) {
	t.Parallel()

	boom := errors.New("boom")
	tr := Adapt(
		fromMethod,
		toMethod,
		func(*workflowservice.DescribeNamespaceRequest) (*workflowservice.GetSystemInfoRequest, error) {
			return nil, boom
		},
		okResponse,
	)

	_, err := tr.request(&workflowservice.DescribeNamespaceRequest{})
	require.ErrorIs(t, err, boom)
}

func TestAdaptGivesTheResponseConverterTheOriginalRequest(t *testing.T) {
	t.Parallel()

	// A field the upstream request cannot carry can only be honoured on the way
	// back, which is why the response converter is handed the request too.
	tr := Adapt(fromMethod, toMethod, okRequest, okResponse)

	reply := &workflowservice.DescribeNamespaceResponse{}
	err := tr.response(
		&workflowservice.DescribeNamespaceRequest{Namespace: "payments"},
		&workflowservice.GetSystemInfoResponse{ServerVersion: "1.2.3"},
		reply,
	)
	require.NoError(t, err)
	require.Equal(t, "payments@1.2.3", reply.GetNamespaceInfo().GetName())
}

func TestTranslationAllocatesUpstreamReply(t *testing.T) {
	t.Parallel()

	tr := Adapt(fromMethod, toMethod, okRequest, okResponse)

	reply := tr.reply()
	require.IsType(t, &workflowservice.GetSystemInfoResponse{}, reply)
	require.NotSame(t, reply, tr.reply(), "each call allocates its own reply")
}

func TestWithHeaderStampsOnlyWhenDeclared(t *testing.T) {
	t.Parallel()

	tr := Adapt(fromMethod, toMethod, okRequest, okResponse).WithHeader("x-api-version", "v1")

	md, ok := metadata.FromOutgoingContext(tr.stamp(t.Context()))
	require.True(t, ok)
	require.Equal(t, []string{"v1"}, md.Get("x-api-version"))

	// A translation that declares no header leaves the context alone.
	plain := Adapt(fromMethod, toMethod, okRequest, okResponse)
	ctx := t.Context()
	require.Equal(t, ctx, plain.stamp(ctx))
}

func TestWithHeaderReplacesAnInboundValue(t *testing.T) {
	t.Parallel()

	tr := Adapt(fromMethod, toMethod, okRequest, okResponse).WithHeader("x-api-version", "v2")

	// The caller's own value was forwarded onto the outgoing context. The
	// translation pins what its conversions were written against, and must leave
	// exactly one value rather than appending a second.
	ctx := metadata.NewOutgoingContext(t.Context(), metadata.Pairs("x-api-version", "v1"))

	md, ok := metadata.FromOutgoingContext(tr.stamp(ctx))
	require.True(t, ok)
	require.Equal(t, []string{"v2"}, md.Get("x-api-version"))
}

func okRequest(*workflowservice.DescribeNamespaceRequest) (*workflowservice.GetSystemInfoRequest, error) {
	return &workflowservice.GetSystemInfoRequest{}, nil
}

// okResponse folds both the original request and the upstream reply into the
// caller's, so a test can tell which of them a value came from.
func okResponse(
	req *workflowservice.DescribeNamespaceRequest,
	up *workflowservice.GetSystemInfoResponse,
	reply *workflowservice.DescribeNamespaceResponse,
) error {
	reply.NamespaceInfo = &namespacepb.NamespaceInfo{Name: req.GetNamespace() + "@" + up.GetServerVersion()}
	return nil
}

// mustProto fails the test unless m is the message the caller expected.
func mustProto[T proto.Message](t *testing.T, m any) T {
	t.Helper()

	out, ok := m.(T)
	require.True(t, ok, "got %T", m)
	return out
}
