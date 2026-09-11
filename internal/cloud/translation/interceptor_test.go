package translation_test

import (
	"context"
	"errors"
	"net"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	workflowservice "go.temporal.io/api/workflowservice/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/temporalio/temporal-proxy/internal/cloud/translation"
)

// systemInfoService is a fake upstream serving the method the test translation
// substitutes onto. It records what it was called with, so a test can tell a
// substituted call from one that was forwarded unchanged.
type systemInfoService struct {
	workflowservice.UnimplementedWorkflowServiceServer

	lis     net.Listener
	version string
	err     error

	// mu guards the recorded state, which the serving goroutine writes while the
	// test reads.
	mu     sync.Mutex
	md     metadata.MD
	called bool
}

func TestDialOptionsSubstitutesTheUpstreamCall(t *testing.T) {
	t.Parallel()

	upstream := newSystemInfoService(t)
	upstream.version = "1.2.3"
	cc := dial(t, upstream, translation.DialOptions(testRegistry(t))...)

	// The reply is built from both halves: "payments" came from the caller's
	// request and "1.2.3" from the substituted call's reply, so one assertion
	// covers the substitution and the conversion in both directions.
	reply := &workflowservice.DescribeNamespaceResponse{}
	err := cc.Invoke(t.Context(), fromMethod, &workflowservice.DescribeNamespaceRequest{Namespace: "payments"}, reply)
	require.NoError(t, err)

	require.Equal(t, "payments@1.2.3", reply.GetNamespaceInfo().GetName())
	require.True(t, upstream.wasCalled(), "the substituted method must have been invoked")
}

func TestDialOptionsPassesUntranslatedMethodsThrough(t *testing.T) {
	t.Parallel()

	// A method the registry does not translate reaches the upstream under its own
	// name, which the fake does not implement.
	const untranslated = "/temporal.api.workflowservice.v1.WorkflowService/ListNamespaces"

	upstream := newSystemInfoService(t)
	cc := dial(t, upstream, translation.DialOptions(testRegistry(t))...)

	err := cc.Invoke(
		t.Context(), untranslated,
		&workflowservice.ListNamespacesRequest{}, &workflowservice.ListNamespacesResponse{},
	)
	require.Equal(t, codes.Unimplemented, status.Code(err))
	require.False(t, upstream.wasCalled(), "and must not be substituted onto one that is implemented")
}

func TestDialOptionsReturnsTheUpstreamError(t *testing.T) {
	t.Parallel()

	upstream := newSystemInfoService(t)
	upstream.err = status.Error(codes.PermissionDenied, "no")
	cc := dial(t, upstream, translation.DialOptions(testRegistry(t))...)

	err := cc.Invoke(
		t.Context(), fromMethod,
		&workflowservice.DescribeNamespaceRequest{}, &workflowservice.DescribeNamespaceResponse{},
	)
	require.Equal(t, codes.PermissionDenied, status.Code(err), "the caller sees the upstream's own status")
	require.Equal(t, "no", status.Convert(err).Message())
}

func TestDialOptionsReportsConversionFailureAsInternal(t *testing.T) {
	t.Parallel()

	boom := errors.New("boom")
	r, err := translation.NewRegistry(translation.Adapt(
		fromMethod, toMethod,
		func(*workflowservice.DescribeNamespaceRequest) (*workflowservice.GetSystemInfoRequest, error) {
			return nil, boom
		},
		okResponse,
	))
	require.NoError(t, err)

	upstream := newSystemInfoService(t)
	cc := dial(t, upstream, translation.DialOptions(r)...)

	err = cc.Invoke(
		t.Context(), fromMethod,
		&workflowservice.DescribeNamespaceRequest{}, &workflowservice.DescribeNamespaceResponse{},
	)
	require.Equal(t, codes.Internal, status.Code(err), "a compiled-in mapping that fails is a proxy bug")
	require.ErrorContains(t, err, "boom")
	require.False(t, upstream.wasCalled(), "and the upstream is never reached")
}

func TestDialOptionsReportsAMismatchedReplyAsInternal(t *testing.T) {
	t.Parallel()

	upstream := newSystemInfoService(t)
	cc := dial(t, upstream, translation.DialOptions(testRegistry(t))...)

	// A reply of the wrong type for the registered translation: the request
	// converts, the substituted call succeeds, and folding the reply back fails.
	err := cc.Invoke(
		t.Context(), fromMethod,
		&workflowservice.DescribeNamespaceRequest{}, &workflowservice.ListNamespacesResponse{},
	)
	require.Equal(t, codes.Internal, status.Code(err))
	require.ErrorContains(t, err, "wanted a temporal.api.workflowservice.v1.DescribeNamespaceResponse reply")
}

func TestDialOptionsReportsAMismatchedRequestAsInternal(t *testing.T) {
	t.Parallel()

	upstream := newSystemInfoService(t)
	cc := dial(t, upstream, translation.DialOptions(testRegistry(t))...)

	// A proto request, but not the one this translation converts. The registry
	// matched on the method, so the mismatch can only be caught by the conversion.
	err := cc.Invoke(
		t.Context(), fromMethod,
		&workflowservice.ListNamespacesRequest{}, &workflowservice.DescribeNamespaceResponse{},
	)
	require.Equal(t, codes.Internal, status.Code(err))
	require.ErrorContains(t, err, "wanted a temporal.api.workflowservice.v1.DescribeNamespaceRequest request")
	require.False(t, upstream.wasCalled(), "and the upstream is never reached")
}

func TestDialOptionsForwardsNonProtoCallsUnchanged(t *testing.T) {
	t.Parallel()

	// Nothing the proxy does reaches here - the reflective forwarder types every
	// message from the proto registry, and a generated client only ever passes
	// proto messages - so this drives the interceptor directly with values no
	// codec can marshal. What matters is that the call is left alone rather than
	// substituted: it fails as an unmarshalable call to the method the caller
	// named, not as a translated one.
	upstream := newSystemInfoService(t)
	cc := dial(t, upstream, translation.DialOptions(testRegistry(t))...)

	req, reply := 42, 0
	err := cc.Invoke(t.Context(), fromMethod, req, &reply)
	// gRPC's own codec rejects it, which is what "left alone" looks like from
	// here: had the call been translated, it would have failed in a conversion
	// with a "wanted a ... request" message instead.
	require.ErrorContains(t, err, "error while marshaling")
	require.False(t, upstream.wasCalled(), "and nothing was substituted onto the upstream method")
}

func TestDialOptionsStampsHeadersOnTheSubstitutedCallOnly(t *testing.T) {
	t.Parallel()

	const header = "x-api-version"

	r, err := translation.NewRegistry(
		translation.Adapt(fromMethod, toMethod, okRequest, okResponse).WithHeader(header, "pinned"),
	)
	require.NoError(t, err)

	upstream := newSystemInfoService(t)
	cc := dial(t, upstream, translation.DialOptions(r)...)

	// The caller sent its own value, which the substituted API cannot be assumed
	// to understand; the translation pins what its conversions were written
	// against, and must leave exactly one value rather than appending a second.
	ctx := metadata.NewOutgoingContext(t.Context(), metadata.Pairs(header, "caller"))
	err = cc.Invoke(ctx, fromMethod, &workflowservice.DescribeNamespaceRequest{}, &workflowservice.DescribeNamespaceResponse{})
	require.NoError(t, err)
	require.Equal(t, []string{"pinned"}, upstream.metadata().Get(header))
}

func TestDialOptionsLeavesAnUntranslatedCallsHeadersAlone(t *testing.T) {
	t.Parallel()

	const header = "x-api-version"

	// The same translation, but invoked as the substituted method directly - a
	// caller speaking that API already, whose own version must travel.
	r, err := translation.NewRegistry(
		translation.Adapt(fromMethod, toMethod, okRequest, okResponse).WithHeader(header, "pinned"),
	)
	require.NoError(t, err)

	upstream := newSystemInfoService(t)
	cc := dial(t, upstream, translation.DialOptions(r)...)

	ctx := metadata.NewOutgoingContext(t.Context(), metadata.Pairs(header, "caller"))
	err = cc.Invoke(ctx, toMethod, &workflowservice.GetSystemInfoRequest{}, &workflowservice.GetSystemInfoResponse{})
	require.NoError(t, err)
	require.Equal(t, []string{"caller"}, upstream.metadata().Get(header))
}

func TestViaSendsTheSubstitutedCallElsewhere(t *testing.T) {
	t.Parallel()

	// Via is what lets a translation answer over a connection other than the one
	// it is installed on, for an upstream method the original connection's service
	// does not serve at all.
	elsewhere := newSystemInfoService(t)
	elsewhere.version = "9.9.9"
	elsewhereConn := dial(t, elsewhere)

	installed := newSystemInfoService(t)
	cc := dial(t, installed, translation.DialOptions(testRegistry(t), translation.Via(elsewhereConn))...)

	reply := &workflowservice.DescribeNamespaceResponse{}
	err := cc.Invoke(t.Context(), fromMethod, &workflowservice.DescribeNamespaceRequest{Namespace: "payments"}, reply)
	require.NoError(t, err)

	require.Equal(t, "payments@9.9.9", reply.GetNamespaceInfo().GetName())
	require.True(t, elsewhere.wasCalled(), "the substituted call goes to the Via connection")
	require.False(t, installed.wasCalled(), "and leaves the chain rather than continuing down it")
}

// testRegistry holds the one test translation.
func testRegistry(t *testing.T) *translation.Registry {
	t.Helper()

	r, err := translation.NewRegistry(translation.Adapt(fromMethod, toMethod, okRequest, okResponse))
	require.NoError(t, err)

	return r
}

// dial returns a client connection to up carrying opts, which are the dial
// options under test. Passing none gives a plain connection, for use as a Via
// target.
func dial(t *testing.T, up *systemInfoService, opts ...grpc.DialOption) *grpc.ClientConn {
	t.Helper()

	cc, err := grpc.NewClient(
		up.lis.Addr().String(),
		append([]grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}, opts...)...,
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = cc.Close() })

	return cc
}

// newSystemInfoService starts the fake on a loopback port and stops it when the
// test ends.
func newSystemInfoService(t *testing.T) *systemInfoService {
	t.Helper()

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	svc := &systemInfoService{lis: lis}

	svr := grpc.NewServer()
	workflowservice.RegisterWorkflowServiceServer(svr, svc)
	go func() { _ = svr.Serve(lis) }()
	t.Cleanup(svr.Stop)

	return svc
}

func (s *systemInfoService) GetSystemInfo(
	ctx context.Context, _ *workflowservice.GetSystemInfoRequest,
) (*workflowservice.GetSystemInfoResponse, error) {
	md, _ := metadata.FromIncomingContext(ctx)

	s.mu.Lock()
	s.md = md
	s.called = true
	s.mu.Unlock()

	if s.err != nil {
		return nil, s.err
	}

	return &workflowservice.GetSystemInfoResponse{ServerVersion: s.version}, nil
}

func (s *systemInfoService) wasCalled() bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.called
}

func (s *systemInfoService) metadata() metadata.MD {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.md.Copy()
}
