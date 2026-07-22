package connect_test

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/temporalio/temporal-proxy/internal/transport/connect"
)

type fakeResolver struct {
	static      bool
	key, target string
	opts        []grpc.DialOption
	err         error
}

func TestStaticResolver(t *testing.T) {
	t.Parallel()

	r := connect.StaticResolver("host:7233", insecureOpt())
	require.True(t, r.IsStatic())

	key, target, opts, err := r.Resolve(t.Context())
	require.NoError(t, err)
	require.Equal(t, "host:7233", key, "cache key equals the address")
	require.Equal(t, "host:7233", target, "dial target equals the address")
	require.Len(t, opts, 1)
}

func TestNewConnEagerLoadsStatic(t *testing.T) {
	t.Parallel()

	pool := connect.NewPool()
	t.Cleanup(func() { _ = pool.Close() })

	_, err := connect.NewConn(pool.ConnOrCreate, connect.StaticResolver("passthrough:///eager", insecureOpt()))
	require.NoError(t, err)

	// Eager load means the connection is already registered in the pool before
	// any request is made.
	conn, err := pool.Conn("passthrough:///eager")
	require.NoError(t, err)
	require.NotNil(t, conn)
}

func TestNewConnDoesNotDialDynamic(t *testing.T) {
	t.Parallel()

	calls := 0
	_, err := connect.NewConn(countingFactory(&calls, nil, nil), fakeResolver{static: false})
	require.NoError(t, err)
	require.Zero(t, calls, "a dynamic resolver must not dial at construction")
}

func TestNewConnStaticFactoryErrorFailsFast(t *testing.T) {
	t.Parallel()

	calls := 0
	boom := errors.New("boom")

	_, err := connect.NewConn(
		countingFactory(&calls, nil, boom),
		connect.StaticResolver("passthrough:///x", insecureOpt()),
	)
	require.Error(t, err)
	require.ErrorContains(t, err, "failed to initialize connection")
	require.ErrorIs(t, err, boom)
	require.Equal(t, 1, calls)
}

func TestConnResolveErrorPropagates(t *testing.T) {
	t.Parallel()

	calls := 0
	boom := errors.New("resolve failed")

	c, err := connect.NewConn(countingFactory(&calls, nil, nil), fakeResolver{static: false, err: boom})
	require.NoError(t, err, "dynamic resolver is not resolved until a call is made")
	require.ErrorIs(t, c.Invoke(t.Context(), "/svc/Method", nil, nil), boom)

	_, streamErr := c.NewStream(t.Context(), &grpc.StreamDesc{}, "/svc/Method")
	require.ErrorIs(t, streamErr, boom)
	require.Zero(t, calls, "the factory is never reached when resolution fails")
}

func TestConnFactoryErrorPropagates(t *testing.T) {
	t.Parallel()

	calls := 0
	boom := errors.New("dial failed")

	c, err := connect.NewConn(
		countingFactory(&calls, nil, boom),
		fakeResolver{static: false, key: "k", target: "t"},
	)
	require.NoError(t, err)
	require.ErrorIs(t, c.Invoke(t.Context(), "/svc/Method", nil, nil), boom)

	_, streamErr := c.NewStream(t.Context(), &grpc.StreamDesc{}, "/svc/Method")
	require.ErrorIs(t, streamErr, boom)
	require.Equal(t, 2, calls, "both Invoke and NewStream resolve through the factory")
}

func (f fakeResolver) IsStatic() bool { return f.static }

func (f fakeResolver) Resolve(context.Context) (string, string, []grpc.DialOption, error) {
	return f.key, f.target, f.opts, f.err
}

// countingFactory returns a ConnFactory that records how many times it is
// invoked and always yields conn/err.
func countingFactory(calls *int, conn *grpc.ClientConn, err error) connect.ConnFactory {
	return func(string, string, ...grpc.DialOption) (*grpc.ClientConn, error) {
		*calls++
		return conn, err
	}
}

func insecureOpt() grpc.DialOption {
	return grpc.WithTransportCredentials(insecure.NewCredentials())
}
