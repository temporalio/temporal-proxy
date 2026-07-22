package connect_test

import (
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/temporalio/temporal-proxy/internal/transport/connect"
)

func TestPoolConn(t *testing.T) {
	t.Parallel()

	t.Run("returns ErrKeyNotFound when key is absent", func(t *testing.T) {
		t.Parallel()

		p := connect.NewPool()
		cn, err := p.Conn("missing")
		require.Nil(t, cn)
		require.ErrorIs(t, err, connect.ErrKeyNotFound)
		require.Contains(t, err.Error(), "missing")
	})

	t.Run("returns the connection when key is present", func(t *testing.T) {
		t.Parallel()

		p := connect.NewPool()
		conn := newConn(t)
		require.NoError(t, p.Set("key", conn))

		cn, err := p.Conn("key")
		require.NoError(t, err)
		require.Same(t, conn, cn)
	})
}

func TestPoolSet(t *testing.T) {
	t.Parallel()

	t.Run("stores a new connection", func(t *testing.T) {
		t.Parallel()

		p := connect.NewPool()
		require.NoError(t, p.Set("key", newConn(t)))
	})

	t.Run("rejects a duplicate key", func(t *testing.T) {
		t.Parallel()

		p := connect.NewPool()
		require.NoError(t, p.Set("key", newConn(t)))

		err := p.Set("key", newConn(t))
		require.ErrorIs(t, err, connect.ErrDuplicateKey)
		require.Contains(t, err.Error(), "key")
	})

	t.Run("keeps the original connection after a duplicate is rejected", func(t *testing.T) {
		t.Parallel()

		p := connect.NewPool()
		original := newConn(t)
		require.NoError(t, p.Set("key", original))
		require.Error(t, p.Set("key", newConn(t)))

		cn, err := p.Conn("key")
		require.NoError(t, err)
		require.Same(t, original, cn)
	})
}

func TestPoolClose(t *testing.T) {
	t.Parallel()

	t.Run("closes all connections", func(t *testing.T) {
		t.Parallel()

		p := connect.NewPool()
		require.NoError(t, p.Set("a", newConn(t)))
		require.NoError(t, p.Set("b", newConn(t)))

		require.NoError(t, p.Close())
	})

	t.Run("is safe to call on an empty pool", func(t *testing.T) {
		t.Parallel()

		require.NoError(t, connect.NewPool().Close())
	})

	t.Run("only closes once", func(t *testing.T) {
		t.Parallel()

		p := connect.NewPool()
		conn := newConn(t)
		require.NoError(t, p.Set("key", conn))

		// First close shuts the underlying connection down. A second close on
		// the conn itself would error, so this proves Close short-circuits via
		// closeOnce instead of re-closing the connections.
		require.NoError(t, p.Close())
		require.NoError(t, p.Close())
	})

	t.Run("aggregates errors from closing connections", func(t *testing.T) {
		t.Parallel()

		p := connect.NewPool()
		conn := newConn(t)
		require.NoError(t, p.Set("key", conn))

		// Close the connection out from under the pool so the pool's own
		// Close observes an "already closed" error and surfaces it.
		require.NoError(t, conn.Close())
		require.Error(t, p.Close())
	})
}

func TestPoolConcurrent(t *testing.T) {
	t.Parallel()

	// This test makes no assertions about ordering; its job is to drive every
	// Pool method from many goroutines at once so the race detector flags any
	// unsynchronized access. It only proves something when run under
	// `go test -race` (mise run test enables it).
	p := connect.NewPool()
	t.Cleanup(func() { require.NoError(t, p.Close()) })

	const (
		workers = 50
		keys    = 8
	)

	var wg sync.WaitGroup
	start := make(chan struct{})

	for i := range workers {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()

			key := fmt.Sprintf("key-%d", i%keys)
			conn := newConn(t)

			<-start // line every goroutine up so the operations actually overlap

			// Multiple goroutines fight over each key: one wins, the rest get
			// ErrDuplicateKey. Close the connection we couldn't hand off so it
			// doesn't leak.
			if err := p.Set(key, conn); err != nil {
				require.ErrorIs(t, err, connect.ErrDuplicateKey)
				require.NoError(t, conn.Close())
			}

			// Conn may or may not find the key yet depending on scheduling;
			// both outcomes are valid, we only care that the read is race-free.
			if _, err := p.Conn(key); err != nil {
				require.ErrorIs(t, err, connect.ErrKeyNotFound)
			}

			// Close racing against Set/Conn must also be safe. closeOnce means
			// only the first caller does the work; the rest are no-ops.
			require.NoError(t, p.Close())
		}(i)
	}

	close(start)
	wg.Wait()
}

func TestPoolConnOrCreate(t *testing.T) {
	t.Parallel()

	insecureOpt := grpc.WithTransportCredentials(insecure.NewCredentials())

	t.Run("creates and registers a connection when key is absent", func(t *testing.T) {
		t.Parallel()

		p := connect.NewPool()
		t.Cleanup(func() { require.NoError(t, p.Close()) })

		conn, err := p.ConnOrCreate("key", "target", insecureOpt)
		require.NoError(t, err)
		require.NotNil(t, conn)

		// The connection is registered, so a subsequent read returns the same one.
		cn, err := p.Conn("key")
		require.NoError(t, err)
		require.Same(t, conn, cn)
	})

	t.Run("returns the existing connection when key is present", func(t *testing.T) {
		t.Parallel()

		p := connect.NewPool()
		t.Cleanup(func() { require.NoError(t, p.Close()) })

		existing := newConn(t)
		require.NoError(t, p.Set("key", existing))

		conn, err := p.ConnOrCreate("key", "target", insecureOpt)
		require.NoError(t, err)
		require.Same(t, existing, conn)
	})

	t.Run("returns an error when dialing fails", func(t *testing.T) {
		t.Parallel()

		p := connect.NewPool()
		t.Cleanup(func() { require.NoError(t, p.Close()) })

		// No transport credentials option, so grpc.NewClient fails synchronously.
		conn, err := p.ConnOrCreate("key", "target")
		require.Nil(t, conn)
		require.ErrorContains(t, err, "failed to connect")
	})

	t.Run("same target with different keys yields distinct connections", func(t *testing.T) {
		t.Parallel()

		// A templated serverName can vary independently of the dial address.
		// Two logical keys sharing a target must not collapse onto the same
		// pooled connection, or one caller's TLS identity would silently leak
		// onto another's channel.
		p := connect.NewPool()
		t.Cleanup(func() { require.NoError(t, p.Close()) })

		connA, err := p.ConnOrCreate("key-a", "shared-target", insecureOpt)
		require.NoError(t, err)

		connB, err := p.ConnOrCreate("key-b", "shared-target", insecureOpt)
		require.NoError(t, err)

		require.NotSame(t, connA, connB)
	})

	t.Run("same key returns the same connection regardless of target", func(t *testing.T) {
		t.Parallel()

		p := connect.NewPool()
		t.Cleanup(func() { require.NoError(t, p.Close()) })

		connA, err := p.ConnOrCreate("key", "target-a", insecureOpt)
		require.NoError(t, err)

		connB, err := p.ConnOrCreate("key", "target-b", insecureOpt)
		require.NoError(t, err)

		require.Same(t, connA, connB)
	})

	t.Run("hands every concurrent caller the same connection", func(t *testing.T) {
		t.Parallel()

		// Many goroutines race to create the same key. Only one dial can be
		// retained; the losers must be closed and every caller must receive the
		// winner. This only proves something under `go test -race`.
		p := connect.NewPool()
		t.Cleanup(func() { require.NoError(t, p.Close()) })

		const workers = 50

		var wg sync.WaitGroup
		start := make(chan struct{})
		conns := make([]*grpc.ClientConn, workers)

		for i := range workers {
			wg.Go(func() {
				<-start // line every goroutine up so the calls actually overlap

				conn, err := p.ConnOrCreate("key", "target", insecureOpt)
				require.NoError(t, err)
				conns[i] = conn
			})
		}

		close(start)
		wg.Wait()

		for i := range workers {
			require.Same(t, conns[0], conns[i])
		}
	})
}

func newConn(t *testing.T) *grpc.ClientConn {
	t.Helper()

	conn, err := grpc.NewClient(
		"passthrough:///test",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)

	require.NoError(t, err)
	return conn
}
