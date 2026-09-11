package translation_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	namespacepb "go.temporal.io/api/namespace/v1"
	workflowservice "go.temporal.io/api/workflowservice/v1"

	"github.com/temporalio/temporal-proxy/internal/cloud/translation"
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
	r, err := translation.NewRegistry(
		translation.Adapt("pkg.Service/From", "pkg.Other/To", okRequest, okResponse),
	)
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
		in   []*translation.Translation
		want string
	}{
		"nil entry": {
			in:   []*translation.Translation{nil},
			want: "nil translation at index 0",
		},
		"malformed from": {
			in:   []*translation.Translation{translation.Adapt("NotAMethod", toMethod, okRequest, okResponse)},
			want: `"NotAMethod" is not a gRPC full method`,
		},
		"malformed to": {
			in:   []*translation.Translation{translation.Adapt(fromMethod, "NotAMethod", okRequest, okResponse)},
			want: `"NotAMethod" is not a gRPC full method`,
		},
		"self mapping": {
			in:   []*translation.Translation{translation.Adapt(fromMethod, fromMethod, okRequest, okResponse)},
			want: "translates onto itself",
		},
		"duplicate from": {
			in: []*translation.Translation{
				translation.Adapt(fromMethod, toMethod, okRequest, okResponse),
				translation.Adapt(fromMethod, "/pkg.Third/To", okRequest, okResponse),
			},
			want: "is translated twice",
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			r, err := translation.NewRegistry(tc.in...)
			require.ErrorContains(t, err, tc.want)
			require.Nil(t, r)
		})
	}
}

func TestRegistryLookupMisses(t *testing.T) {
	t.Parallel()

	var nilRegistry *translation.Registry
	_, ok := nilRegistry.Lookup(fromMethod)
	require.False(t, ok, "a nil registry translates nothing")
	require.Nil(t, nilRegistry.Methods())

	empty, err := translation.NewRegistry()
	require.NoError(t, err)
	_, ok = empty.Lookup(fromMethod)
	require.False(t, ok)
	require.Empty(t, empty.Methods())
}

// okRequest converts the caller's request into the substituted one. It carries
// nothing across, since what the conversions do is not what these tests are
// about.
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
