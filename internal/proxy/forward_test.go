package proxy

import (
	"context"
	"errors"
	"fmt"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
	workflowservice "go.temporal.io/api/workflowservice/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"

	"github.com/temporalio/temporal-proxy/internal/protoutil"
	"github.com/temporalio/temporal-proxy/internal/services"
	"github.com/temporalio/temporal-proxy/internal/transport/meta"
	"github.com/temporalio/temporal-proxy/pkg/testutil"
)

type (
	// partialTypes resolves every message except those named in missing, so a test
	// can knock out just a request type or just a response type.
	partialTypes struct {
		missing map[protoreflect.FullName]struct{}
	}
)

func TestForwardContextCarriesIncomingMetadataOutbound(t *testing.T) {
	t.Parallel()

	// Guard: incoming metadata must reach the outbound call. Templated upstream
	// resolution reads the router-stamped namespace from the outgoing context and
	// namespace translation rewrites the temporal-namespace header, so dropping
	// incoming metadata here silently breaks both.
	in := metadata.Pairs(
		meta.NamespaceHeader, "orders",
		temporalNamespaceHeader, "orders",
		"authorization", "Bearer k3y",
		"user-agent", "grpc-go/1.0",
		"content-type", "application/grpc",
		":authority", "127.0.0.1:7233",
	)

	out, ok := metadata.FromOutgoingContext(forwardContext(metadata.NewIncomingContext(t.Context(), in)))
	require.True(t, ok, "expected forwardContext to install outgoing metadata")
	require.Equal(t, []string{"orders"}, out.Get(meta.NamespaceHeader))
	require.Equal(t, []string{"orders"}, out.Get(temporalNamespaceHeader))
	require.Equal(t, []string{"Bearer k3y"}, out.Get("authorization"))

	// Transport headers describe the inbound hop: gRPC sets its own on the
	// outbound call and rejects a pseudo-header supplied as metadata.
	for _, key := range transportHeaders {
		require.Empty(t, out.Get(key), "expected %q to be stripped", key)
	}
}

func TestForwardContextKeepsExistingOutgoingValue(t *testing.T) {
	t.Parallel()

	// An outgoing value already set for a key wins, so a caller-supplied header
	// cannot displace one the proxy stamped itself.
	ctx := metadata.NewIncomingContext(t.Context(), metadata.Pairs("authorization", "Bearer caller"))
	ctx = metadata.NewOutgoingContext(ctx, metadata.Pairs("authorization", "Bearer upstream"))

	out, ok := metadata.FromOutgoingContext(forwardContext(ctx))
	require.True(t, ok)
	require.Equal(t, []string{"Bearer upstream"}, out.Get("authorization"))
}

func TestForwardContextWithoutIncomingMetadata(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	require.Equal(t, ctx, forwardContext(ctx))
}

func TestNewForwarderValidation(t *testing.T) {
	t.Parallel()

	gate := services.NewAllowlist(services.Default())

	tests := []struct {
		name string
		cc   grpc.ClientConnInterface
		gate Gate
		err  string
	}{
		{name: "nil client connection", gate: gate, err: "nil client connection"},
		{name: "nil gate", cc: &testutil.ClientConn{}, err: "nil gate"},
		{name: "both nil reports the connection first", err: "nil client connection"},
		{name: "a connection and a gate is enough", cc: &testutil.ClientConn{}, gate: gate},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			fw, err := NewForwarder(tt.cc, tt.gate)
			if tt.err != "" {
				require.ErrorContains(t, err, tt.err)
				require.Nil(t, fw)

				return
			}

			require.NoError(t, err)
			require.Equal(t, protoregistry.GlobalTypes, fw.types, "expected the global registry by default")
		})
	}
}

func TestWithProtoTypes(t *testing.T) {
	t.Parallel()

	custom := partialTypes{}

	fw, err := NewForwarder(&testutil.ClientConn{}, services.NewAllowlist(services.Default()), WithProtoTypes(custom))
	require.NoError(t, err)
	require.Equal(t, custom, fw.types)

	// A nil registry leaves the default in place rather than disabling resolution.
	fw, err = NewForwarder(&testutil.ClientConn{}, services.NewAllowlist(services.Default()), WithProtoTypes(nil))
	require.NoError(t, err)
	require.Equal(t, protoregistry.GlobalTypes, fw.types)
}

func TestStatusError(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		err  error
		code codes.Code
		msg  string
	}{
		{name: "nil stays nil", code: codes.OK},
		{
			name: "an error already carrying a status is forwarded verbatim",
			err:  status.Error(codes.NotFound, "namespace not found"),
			code: codes.NotFound,
			msg:  "namespace not found",
		},
		{
			name: "a raw cancellation maps to its status",
			err:  context.Canceled,
			code: codes.Canceled,
		},
		{
			name: "a raw deadline maps to its status",
			err:  context.DeadlineExceeded,
			code: codes.DeadlineExceeded,
		},
		{
			// io.EOF reaches here when a caller half-closes mid-request and carries no
			// status of its own, so it must not surface as an opaque Unknown.
			name: "io.EOF is reported as Internal naming the step",
			err:  io.EOF,
			code: codes.Internal,
			msg:  "proxy: sending the response failed: EOF",
		},
		{
			name: "anything else is reported as Internal naming the step",
			err:  errors.New("boom"),
			code: codes.Internal,
			msg:  "proxy: sending the response failed: boom",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := statusError("sending the response failed", tt.err)
			require.Equal(t, tt.code, status.Code(got))

			if tt.msg == "" {
				return
			}

			require.ErrorContains(t, got, tt.msg)
		})
	}
}

func TestResolveMethod(t *testing.T) {
	t.Parallel()

	const getSystemInfo = "/" + services.WorkflowService + "/GetSystemInfo"

	tests := []struct {
		name  string
		types protoutil.Types
		in    string
		want  protoreflect.FullName
	}{
		{
			name: "a resolvable method yields its descriptor",
			in:   getSystemInfo,
			want: services.WorkflowService + ".GetSystemInfo",
		},
		{name: "a name with no method to split off", in: "GetSystemInfo"},
		{name: "a service that is not linked in", in: "/not.A.Service/Method"},
		{name: "a method the service does not have", in: "/" + services.WorkflowService + "/NoSuchMethod"},
		{
			name:  "a request type missing from the registry",
			types: missingTypes(msgName(&workflowservice.GetSystemInfoRequest{})),
			in:    getSystemInfo,
		},
		{
			name:  "a response type missing from the registry",
			types: missingTypes(msgName(&workflowservice.GetSystemInfoResponse{})),
			in:    getSystemInfo,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			fw, err := NewForwarder(&testutil.ClientConn{}, services.NewAllowlist(services.Default()), WithProtoTypes(tt.types))
			require.NoError(t, err)

			got := fw.resolveMethod(tt.in)
			if tt.want == "" {
				require.Nil(t, got)

				return
			}

			require.NotNil(t, got)
			require.Equal(t, tt.want, got.desc.FullName())
		})
	}
}

func TestLookupCachesResolvedMethods(t *testing.T) {
	t.Parallel()

	fw, err := NewForwarder(&testutil.ClientConn{}, services.NewAllowlist(services.Default()))
	require.NoError(t, err)

	const method = "/" + services.WorkflowService + "/GetSystemInfo"

	first := fw.lookup(method)
	require.NotNil(t, first)
	require.Same(t, first, fw.lookup(method), "expected the second lookup to hit the cache")

	// An unresolvable method is not cached, so a bad name cannot grow the map.
	require.Nil(t, fw.lookup("/"+services.WorkflowService+"/NoSuchMethod"))
	_, cached := fw.methods.Load("/" + services.WorkflowService + "/NoSuchMethod")
	require.False(t, cached)
}

func TestHandleErrors(t *testing.T) {
	t.Parallel()

	const (
		unaryMethod  = "/" + services.WorkflowService + "/GetSystemInfo"
		streamMethod = "/" + services.Reflection + "/ServerReflectionInfo"
	)

	tests := []struct {
		name     string
		method   string
		noStream bool
		allowed  []string
		conn     *testutil.ClientConn
		ss       testutil.ServerStream
		code     codes.Code
		msg      string
	}{
		{
			name:     "a context without a server transport stream",
			noStream: true,
			code:     codes.Internal,
			msg:      "no server transport stream",
		},
		{
			name:    "a service that is not on the allowlist",
			method:  "/" + services.OperatorService + "/DeleteNamespace",
			allowed: []string{services.WorkflowService},
			code:    codes.Unimplemented,
			msg:     "unknown service " + services.OperatorService,
		},
		{
			name:    "a method the compiled descriptors do not have",
			method:  "/" + services.WorkflowService + "/NoSuchMethod",
			allowed: services.Default(),
			code:    codes.Unimplemented,
			msg:     "unknown method",
		},
		{
			name:    "an upstream that rejects the unary call",
			method:  unaryMethod,
			allowed: services.Default(),
			conn:    &testutil.ClientConn{InvokeErr: status.Error(codes.PermissionDenied, "denied")},
			code:    codes.PermissionDenied,
			msg:     "denied",
		},
		{
			name:    "a caller that cannot receive the relayed header",
			method:  unaryMethod,
			allowed: services.Default(),
			conn:    &testutil.ClientConn{Header: metadata.Pairs("x-upstream", "1")},
			ss:      testutil.ServerStream{HeaderErr: errors.New("header refused")},
			code:    codes.Internal,
			msg:     "header refused",
		},
		{
			name:    "a caller that cannot receive the response",
			method:  unaryMethod,
			allowed: services.Default(),
			ss:      testutil.ServerStream{SendErr: errors.New("broken pipe")},
			code:    codes.Internal,
			msg:     "broken pipe",
		},
		{
			name:    "an upstream stream that cannot be opened",
			method:  streamMethod,
			allowed: []string{services.Reflection},
			conn:    &testutil.ClientConn{StreamErr: status.Error(codes.Unavailable, "upstream down")},
			code:    codes.Unavailable,
			msg:     "upstream down",
		},
		{
			name:    "an upstream stream that fails before its header",
			method:  streamMethod,
			allowed: []string{services.Reflection},
			conn:    &testutil.ClientConn{Stream: &testutil.ClientStream{HeaderErr: status.Error(codes.Aborted, "header failed")}},
			ss:      testutil.ServerStream{RecvErr: io.EOF},
			code:    codes.Aborted,
			msg:     "header failed",
		},
		{
			// The request pump reports first because the response pump is parked in
			// RecvMsg, so this pins the mapping of a caller-side stream failure.
			name:    "a caller whose request stream breaks",
			method:  streamMethod,
			allowed: []string{services.Reflection},
			conn:    &testutil.ClientConn{Stream: &testutil.ClientStream{BlockRecv: make(chan struct{})}},
			ss:      testutil.ServerStream{RecvErr: errors.New("caller vanished")},
			code:    codes.Internal,
			msg:     "caller vanished",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			conn := tt.conn
			if conn == nil {
				conn = &testutil.ClientConn{}
			}

			if cs, ok := conn.Stream.(*testutil.ClientStream); ok && cs.BlockRecv != nil {
				t.Cleanup(func() { close(cs.BlockRecv) })
			}

			fw, err := NewForwarder(conn, services.NewAllowlist(tt.allowed))
			require.NoError(t, err)

			ss := tt.ss
			ss.Ctx = t.Context()
			if !tt.noStream {
				ss.Ctx = grpc.NewContextWithServerTransportStream(ss.Ctx, testutil.ServerTransportStream{FullMethodName: tt.method})
			}

			err = fw.Handle(nil, ss)
			require.Equal(t, tt.code, status.Code(err))
			require.ErrorContains(t, err, tt.msg)
		})
	}
}

func TestStreamTreatsWrappedEOFAsHalfClose(t *testing.T) {
	t.Parallel()

	// The pumps forward whatever the streams hand back, so a wrapped io.EOF has to
	// read as a clean end of stream. Comparing with == instead of errors.Is turns
	// one into a forwarding failure, which the caller sees as Internal on an
	// otherwise successful call.
	wrapped := fmt.Errorf("transport closed: %w", io.EOF)

	fw, err := NewForwarder(
		&testutil.ClientConn{Stream: &testutil.ClientStream{RecvErr: wrapped}},
		services.NewAllowlist([]string{services.Reflection}),
	)
	require.NoError(t, err)

	ss := testutil.ServerStream{RecvErr: wrapped}
	ss.Ctx = grpc.NewContextWithServerTransportStream(
		t.Context(),
		testutil.ServerTransportStream{FullMethodName: "/" + services.Reflection + "/ServerReflectionInfo"},
	)

	require.NoError(t, fw.Handle(nil, ss))
}

func TestPumpRequestsReportsFailures(t *testing.T) {
	t.Parallel()

	in := messageType(t, &workflowservice.GetSystemInfoRequest{})

	tests := []struct {
		name string
		src  grpc.ServerStream
		dst  grpc.ClientStream
		want string
	}{
		{
			name: "a clean half-close reports io.EOF",
			src:  testutil.ServerStream{RecvErr: io.EOF},
			want: io.EOF.Error(),
		},
		{
			name: "a failed receive is reported",
			src:  testutil.ServerStream{RecvErr: errors.New("recv failed")},
			want: "recv failed",
		},
		{
			name: "a failed send upstream is reported",
			dst:  &testutil.ClientStream{SendErr: errors.New("send failed")},
			want: "send failed",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// The side a case does not configure succeeds, rather than being a nil
			// interface the pump would dereference.
			src, dst := tt.src, tt.dst
			if src == nil {
				src = testutil.ServerStream{}
			}

			if dst == nil {
				dst = &testutil.ClientStream{}
			}

			require.ErrorContains(t, <-pumpRequests(src, dst, in), tt.want)
		})
	}
}

func TestPumpResponsesReportsFailures(t *testing.T) {
	t.Parallel()

	out := messageType(t, &workflowservice.GetSystemInfoResponse{})

	tests := []struct {
		name string
		src  grpc.ClientStream
		dst  grpc.ServerStream
		want string
	}{
		{
			name: "a failed upstream header is reported",
			src:  &testutil.ClientStream{HeaderErr: errors.New("header failed")},
			want: "header failed",
		},
		{
			name: "a header the caller refuses is reported",
			dst:  testutil.ServerStream{HeaderErr: errors.New("send header failed")},
			want: "send header failed",
		},
		{
			name: "a clean completion reports io.EOF",
			src:  &testutil.ClientStream{RecvErr: io.EOF},
			want: io.EOF.Error(),
		},
		{
			name: "a failed upstream receive is reported",
			src:  &testutil.ClientStream{RecvErr: errors.New("recv failed")},
			want: "recv failed",
		},
		{
			name: "a response the caller refuses is reported",
			dst:  testutil.ServerStream{SendErr: errors.New("send failed")},
			want: "send failed",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// The side a case does not configure succeeds, rather than being a nil
			// interface the pump would dereference.
			src, dst := tt.src, tt.dst
			if src == nil {
				src = &testutil.ClientStream{}
			}

			if dst == nil {
				dst = testutil.ServerStream{}
			}

			require.ErrorContains(t, <-pumpResponses(src, dst, out), tt.want)
		})
	}
}

func (p partialTypes) FindMessageByName(name protoreflect.FullName) (protoreflect.MessageType, error) {
	if _, ok := p.missing[name]; ok {
		return nil, protoregistry.NotFound
	}

	return protoregistry.GlobalTypes.FindMessageByName(name)
}

// missingTypes returns a registry that resolves everything except name.
func missingTypes(name protoreflect.FullName) partialTypes {
	return partialTypes{missing: map[protoreflect.FullName]struct{}{name: {}}}
}

// msgName returns m's fully qualified proto message name, derived from the
// generated type so no literal can drift from the descriptors.
func msgName(m proto.Message) protoreflect.FullName {
	return m.ProtoReflect().Descriptor().FullName()
}

// messageType resolves the registered type for m, as the pumps do per frame.
func messageType(t *testing.T, m proto.Message) protoreflect.MessageType {
	t.Helper()

	mt, err := protoregistry.GlobalTypes.FindMessageByName(msgName(m))
	require.NoError(t, err)

	return mt
}
