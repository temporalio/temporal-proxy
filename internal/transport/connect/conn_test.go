package connect_test

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/temporalio/temporal-proxy/internal/transport/connect"
)

type (
	fakeResolver struct {
		static      bool
		key, target string
		opts        []grpc.DialOption
		err         error
	}

	// flakyResolver is static and resolves cleanly exactly once, for
	// [connect.NewConn], then fails. It is the only way to reach the resolve
	// failure inside [connect.Conn.WaitReady], since a resolver that fails outright
	// never yields a Conn to call it on.
	flakyResolver struct {
		err   error
		calls int
	}
)

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

func TestConnWaitReady(t *testing.T) {
	t.Parallel()

	t.Run("opens a static connection", func(t *testing.T) {
		t.Parallel()

		pool := connect.NewPool()
		t.Cleanup(func() { require.NoError(t, pool.Close()) })

		addr := serveTCP(t)
		c, err := connect.NewConn(pool.ConnOrCreate, connect.StaticResolver(addr, insecureOpt()))
		require.NoError(t, err)

		cc, err := pool.Conn(addr)
		require.NoError(t, err)
		require.Equal(t, connectivity.Idle, cc.GetState(), "grpc.NewClient does not dial on its own")

		// Bounded so a connection that never leaves idle fails the test rather
		// than hanging it.
		ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
		defer cancel()

		require.NoError(t, c.WaitReady(ctx))
		require.Equal(t, connectivity.Ready, cc.GetState())
	})

	t.Run("reports the target that never becomes ready", func(t *testing.T) {
		t.Parallel()

		pool := connect.NewPool()
		t.Cleanup(func() { require.NoError(t, pool.Close()) })

		addr := deadAddr(t)
		c, err := connect.NewConn(pool.ConnOrCreate, connect.StaticResolver(addr, insecureOpt()))
		require.NoError(t, err)

		ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
		defer cancel()

		err = c.WaitReady(ctx)
		require.ErrorContains(t, err, addr)
		require.ErrorContains(t, err, "not ready")
		require.ErrorIs(t, err, context.DeadlineExceeded, "the context's cause is wrapped, not just described")
	})

	t.Run("reports a cancelled wait as cancelled", func(t *testing.T) {
		t.Parallel()

		pool := connect.NewPool()
		t.Cleanup(func() { require.NoError(t, pool.Close()) })

		c, err := connect.NewConn(pool.ConnOrCreate, connect.StaticResolver(deadAddr(t), insecureOpt()))
		require.NoError(t, err)

		// fx cancels the start context when another hook fails, so a cancelled wait
		// must not be reported as having run out of time.
		ctx, cancel := context.WithCancel(t.Context())
		cancel()

		err = c.WaitReady(ctx)
		require.ErrorIs(t, err, context.Canceled)
		require.NotErrorIs(t, err, context.DeadlineExceeded)
	})

	t.Run("propagates a resolve failure", func(t *testing.T) {
		t.Parallel()

		boom := errors.New("resolve failed")
		r := &flakyResolver{err: boom}

		c, err := connect.NewConn(func(string, string, ...grpc.DialOption) (*grpc.ClientConn, error) {
			return newConn(t), nil
		}, r)
		require.NoError(t, err)

		require.ErrorIs(t, c.WaitReady(t.Context()), boom)
	})

	t.Run("propagates a factory failure", func(t *testing.T) {
		t.Parallel()

		boom := errors.New("dial failed")
		calls := 0

		// The factory has to succeed for NewConn and fail afterwards, since a
		// static Conn that could not be created never reaches WaitReady.
		c, err := connect.NewConn(func(string, string, ...grpc.DialOption) (*grpc.ClientConn, error) {
			if calls++; calls > 1 {
				return nil, boom
			}

			return newConn(t), nil
		}, connect.StaticResolver("passthrough:///x", insecureOpt()))
		require.NoError(t, err)

		require.ErrorIs(t, c.WaitReady(t.Context()), boom)
	})

	t.Run("does nothing for a dynamic connection", func(t *testing.T) {
		t.Parallel()

		// A dynamic Conn has no connection to open, so this must neither dial nor
		// resolve. t.Context has no deadline, so a regression that waits on
		// something hangs the test rather than passing quietly.
		calls := 0
		c, err := connect.NewConn(countingFactory(&calls, nil, nil), fakeResolver{static: false})
		require.NoError(t, err)

		require.NoError(t, c.WaitReady(t.Context()))
		require.Zero(t, calls)
	})

	t.Run("fails fast on a closed connection", func(t *testing.T) {
		t.Parallel()

		// A closed connection never changes state again, so without the shutdown
		// guard this would block until ctx expired.
		pool := connect.NewPool()

		addr := deadAddr(t)
		c, err := connect.NewConn(pool.ConnOrCreate, connect.StaticResolver(addr, insecureOpt()))
		require.NoError(t, err)
		require.NoError(t, pool.Close())

		require.ErrorContains(t, c.WaitReady(t.Context()), addr+": connection is closed")
	})
}

func TestWaitReady(t *testing.T) {
	t.Parallel()

	t.Run("waits for every connection", func(t *testing.T) {
		t.Parallel()

		pool := connect.NewPool()
		t.Cleanup(func() { require.NoError(t, pool.Close()) })

		conns := make([]*connect.Conn, 0, 3)
		for range 3 {
			c, err := connect.NewConn(pool.ConnOrCreate, connect.StaticResolver(serveTCP(t), insecureOpt()))
			require.NoError(t, err)
			conns = append(conns, c)
		}

		ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
		defer cancel()
		require.NoError(t, connect.WaitReady(ctx, conns...))
	})

	t.Run("reports every unreachable target", func(t *testing.T) {
		t.Parallel()

		pool := connect.NewPool()
		t.Cleanup(func() { require.NoError(t, pool.Close()) })

		alpha, bravo := deadAddr(t), deadAddr(t)
		conns := make([]*connect.Conn, 0, 2)
		for _, addr := range []string{alpha, bravo} {
			c, err := connect.NewConn(pool.ConnOrCreate, connect.StaticResolver(addr, insecureOpt()))
			require.NoError(t, err)
			conns = append(conns, c)
		}

		ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
		defer cancel()

		// Both are named: one unreachable target does not mask another. They are
		// waited on concurrently, so a single deadline covers both.
		err := connect.WaitReady(ctx, conns...)
		require.ErrorContains(t, err, alpha)
		require.ErrorContains(t, err, bravo)
	})

	t.Run("returns nil with no connections", func(t *testing.T) {
		t.Parallel()

		require.NoError(t, connect.WaitReady(t.Context()))
	})
}

func (f fakeResolver) IsStatic() bool { return f.static }

func (f fakeResolver) Resolve(context.Context) (string, string, []grpc.DialOption, error) {
	return f.key, f.target, f.opts, f.err
}

func (r *flakyResolver) IsStatic() bool { return true }

func (r *flakyResolver) Resolve(context.Context) (string, string, []grpc.DialOption, error) {
	if r.calls++; r.calls > 1 {
		return "", "", nil, r.err
	}

	return "key", "passthrough:///flaky", []grpc.DialOption{insecureOpt()}, nil
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

// serveTCP starts a gRPC server on a loopback port and returns its address, so a
// connection to it can actually reach ready.
func serveTCP(t *testing.T) string {
	t.Helper()

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	svr := grpc.NewServer()
	go func() { _ = svr.Serve(lis) }()
	t.Cleanup(svr.Stop)

	return lis.Addr().String()
}

// deadAddr returns a loopback address with nothing behind it, by taking a port
// from the kernel and immediately giving it back.
func deadAddr(t *testing.T) string {
	t.Helper()

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	addr := lis.Addr().String()
	require.NoError(t, lis.Close())

	return addr
}
