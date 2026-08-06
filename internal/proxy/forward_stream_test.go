package proxy_test

import (
	"context"
	"io"
	"reflect"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/reflection"
	reflectionpb "google.golang.org/grpc/reflection/grpc_reflection_v1"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	"github.com/temporalio/temporal-proxy/internal/proxy"
	"github.com/temporalio/temporal-proxy/internal/services"
)

type (
	// streamTypeRecorder captures the concrete Go type of every message sent
	// and received on the upstream connection, so a test can assert the
	// forwarder decoded frames into their proto type rather than relaying
	// opaque bytes. A raw relay would pass every byte-level round-trip
	// assertion in this file while silently disabling namespace translation
	// and payload encryption for every stream, which is the failure this
	// records against.
	streamTypeRecorder struct {
		mu   sync.Mutex
		sent []reflect.Type
		recv []reflect.Type
	}

	// recordingClientStream wraps a client stream to record the concrete type
	// of every message passed through SendMsg and RecvMsg, without altering
	// behavior.
	recordingClientStream struct {
		grpc.ClientStream

		rec *streamTypeRecorder
	}
)

// interceptor returns a grpc.StreamClientInterceptor that wraps every stream
// opened on the connection it is installed on with a recordingClientStream.
func (r *streamTypeRecorder) interceptor() grpc.StreamClientInterceptor {
	return func(
		ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string,
		streamer grpc.Streamer, opts ...grpc.CallOption,
	) (grpc.ClientStream, error) {
		cs, err := streamer(ctx, desc, cc, method, opts...)
		if err != nil {
			return nil, err
		}

		return &recordingClientStream{ClientStream: cs, rec: r}, nil
	}
}

// snapshot returns a copy of the types recorded so far. Safe to call once the
// stream under test has completed: the recorder is written from the
// forwarder's pump goroutines, and completion (an EOF or terminal status
// observed by the caller) only happens after those goroutines stop writing.
func (r *streamTypeRecorder) snapshot() (sent, recv []reflect.Type) {
	r.mu.Lock()
	defer r.mu.Unlock()

	return append([]reflect.Type(nil), r.sent...), append([]reflect.Type(nil), r.recv...)
}

func (s *recordingClientStream) SendMsg(m any) error {
	s.rec.mu.Lock()
	s.rec.sent = append(s.rec.sent, reflect.TypeOf(m))
	s.rec.mu.Unlock()

	return s.ClientStream.SendMsg(m)
}

func (s *recordingClientStream) RecvMsg(m any) error {
	err := s.ClientStream.RecvMsg(m)
	if err == nil {
		s.rec.mu.Lock()
		s.rec.recv = append(s.rec.recv, reflect.TypeOf(m))
		s.rec.mu.Unlock()
	}

	return err
}

// newStreamForwarderConn stands up an upstream serving reflection, a forwarder
// in front of it, and returns a conn to the forwarder along with a recorder of
// the concrete message types the forwarder sent and received on the upstream
// connection.
func newStreamForwarderConn(t *testing.T) (*grpc.ClientConn, *streamTypeRecorder) {
	t.Helper()

	upLis := bufconn.Listen(1024 * 1024)
	upSrv := grpc.NewServer()
	reflection.Register(upSrv)
	go func() { _ = upSrv.Serve(upLis) }()
	t.Cleanup(upSrv.Stop)

	rec := &streamTypeRecorder{}
	upConn := dialBuf(t, upLis, grpc.WithChainStreamInterceptor(rec.interceptor()))

	fwdLis := bufconn.Listen(1024 * 1024)
	fwdSrv := grpc.NewServer(grpc.UnknownServiceHandler(proxy.NewForwarder(upConn, []string{services.Reflection}).Handle))
	go func() { _ = fwdSrv.Serve(fwdLis) }()
	t.Cleanup(fwdSrv.Stop)

	return dialBuf(t, fwdLis), rec
}

func TestForwarderCarriesBidiStreams(t *testing.T) {
	t.Parallel()

	// ServerReflection is the only bidi-streaming service linked into the
	// binary, which makes it the natural fixture: request in, response out,
	// both ends typed.
	conn, rec := newStreamForwarderConn(t)

	stream, err := reflectionpb.NewServerReflectionClient(conn).ServerReflectionInfo(t.Context())
	require.NoError(t, err)

	require.NoError(t, stream.Send(&reflectionpb.ServerReflectionRequest{
		MessageRequest: &reflectionpb.ServerReflectionRequest_ListServices{ListServices: ""},
	}))

	resp, err := stream.Recv()
	require.NoError(t, err)
	require.NotEmpty(t, resp.GetListServicesResponse().GetService(),
		"the upstream's service list must arrive through the forwarder")

	require.NoError(t, stream.CloseSend())
	_, err = stream.Recv()
	require.ErrorIs(t, err, io.EOF, "a clean upstream half-close must reach the caller as EOF")

	sent, recv := rec.snapshot()
	require.Equal(t, []reflect.Type{reflect.TypeFor[*reflectionpb.ServerReflectionRequest]()}, sent,
		"the forwarder must send the upstream a decoded *ServerReflectionRequest, not an opaque frame; "+
			"a raw relay here would silently skip namespace translation and payload encryption for every stream")
	require.Equal(t, []reflect.Type{reflect.TypeFor[*reflectionpb.ServerReflectionResponse]()}, recv,
		"the forwarder must receive a decoded *ServerReflectionResponse from the upstream, not an opaque frame; "+
			"a raw relay here would silently skip namespace translation and payload encryption for every stream")
}

func TestForwarderStreamForwardsOrdinaryErrorResponseFrame(t *testing.T) {
	t.Parallel()

	// This proves an ordinary data frame survives the round trip; it happens
	// to carry reflection's own application-level ErrorResponse, but the pump
	// never inspects the payload, so this is not a test of gRPC status
	// propagation. See TestForwarderStreamPropagatesUpstreamStatus for that.
	conn, _ := newStreamForwarderConn(t)

	stream, err := reflectionpb.NewServerReflectionClient(conn).ServerReflectionInfo(t.Context())
	require.NoError(t, err)

	require.NoError(t, stream.Send(&reflectionpb.ServerReflectionRequest{
		MessageRequest: &reflectionpb.ServerReflectionRequest_FileByFilename{
			FileByFilename: "no/such/file.proto",
		},
	}))

	resp, err := stream.Recv()
	require.NoError(t, err, "reflection reports a missing file in-band, not as a status")
	require.NotNil(t, resp.GetErrorResponse())
}

func TestForwarderStreamPropagatesUpstreamStatus(t *testing.T) {
	t.Parallel()

	conn, _ := newStreamForwarderConn(t)

	stream, err := reflectionpb.NewServerReflectionClient(conn).ServerReflectionInfo(t.Context())
	require.NoError(t, err)

	// An empty MessageRequest is malformed; reflection's own handler answers
	// it with a genuine gRPC status rather than an in-band ErrorResponse,
	// which is what exercises the response pump's real status-propagation
	// path rather than an ordinary frame carrying error-shaped data.
	require.NoError(t, stream.Send(&reflectionpb.ServerReflectionRequest{}))

	_, err = stream.Recv()
	require.Equal(t, codes.InvalidArgument, status.Code(err),
		"the upstream's terminal status must reach the caller intact")
}
