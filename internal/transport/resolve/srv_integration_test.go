package resolve_test

import (
	"errors"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/temporalio/temporal-proxy/internal/transport/resolve"
)

// backend is a gRPC server that answers any method and counts calls.
type backend struct {
	port uint16

	mu        sync.Mutex
	calls     int
	authority string
}

func TestSRVRoundRobin(t *testing.T) {
	t.Parallel()

	a, b := newBackend(t), newBackend(t)
	lk := &fakeLookup{records: []*net.SRV{a.record(), b.record()}}

	conn, err := grpc.NewClient(
		"srv:///_grpc._tcp.backends.test",
		grpc.WithResolvers(resolve.NewTestBuilder(lk.lookup, 50*time.Millisecond, 10*time.Millisecond)),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })

	call := func() error {
		return conn.Invoke(t.Context(), "/test.Echo/Call", &emptypb.Empty{}, &emptypb.Empty{}, grpc.WaitForReady(true))
	}

	require.Eventually(t, func() bool {
		return call() == nil && a.count() > 0 && b.count() > 0
	}, 5*time.Second, 10*time.Millisecond, "round_robin reaches every backend")
	require.Equal(t, "backends.test", a.seenAuthority(), "authority drops the _service._proto labels")

	// DNS fails from here on. Wait for several failed polls, then both backends
	// must still be serving.
	lk.set(nil, errors.New("SERVFAIL"))
	seen := lk.count()
	require.Eventually(t, func() bool { return lk.count() >= seen+3 }, 5*time.Second, 10*time.Millisecond)

	beforeA, beforeB := a.count(), b.count()
	for range 10 {
		require.NoError(t, call())
	}

	require.Greater(t, a.count(), beforeA, "a failed lookup keeps backend a")
	require.Greater(t, b.count(), beforeB, "a failed lookup keeps backend b")
}

// count returns how many calls reached the backend.
func (b *backend) count() int {
	b.mu.Lock()
	defer b.mu.Unlock()

	return b.calls
}

// record is the SRV record that points at the backend.
func (b *backend) record() *net.SRV {
	return &net.SRV{Target: "127.0.0.1.", Port: b.port}
}

// seenAuthority returns the :authority of the last call.
func (b *backend) seenAuthority() string {
	b.mu.Lock()
	defer b.mu.Unlock()

	return b.authority
}

// newBackend starts a backend on a loopback port and stops it when the test ends.
func newBackend(t *testing.T) *backend {
	t.Helper()

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	be := &backend{port: uint16(lis.Addr().(*net.TCPAddr).Port)}
	srv := grpc.NewServer(grpc.UnknownServiceHandler(func(_ any, stream grpc.ServerStream) error {
		md, _ := metadata.FromIncomingContext(stream.Context())

		be.mu.Lock()
		be.calls++
		if v := md.Get(":authority"); len(v) > 0 {
			be.authority = v[0]
		}
		be.mu.Unlock()

		if err := stream.RecvMsg(new(emptypb.Empty)); err != nil {
			return err
		}

		return stream.SendMsg(&emptypb.Empty{})
	}))

	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)

	return be
}
