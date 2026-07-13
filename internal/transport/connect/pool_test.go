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

	t.Run("returns ErrHostNotFound when host is absent", func(t *testing.T) {
		t.Parallel()

		p := connect.NewPool()
		cn, err := p.Conn("missing")
		require.Nil(t, cn)
		require.ErrorIs(t, err, connect.ErrHostNotFound)
		require.Contains(t, err.Error(), "missing")
	})

	t.Run("returns the connection when host is present", func(t *testing.T) {
		t.Parallel()

		p := connect.NewPool()
		conn := newConn(t)
		require.NoError(t, p.Set("host", conn))

		cn, err := p.Conn("host")
		require.NoError(t, err)
		require.Same(t, conn, cn)
	})
}

func TestPoolSet(t *testing.T) {
	t.Parallel()

	t.Run("stores a new connection", func(t *testing.T) {
		t.Parallel()

		p := connect.NewPool()
		require.NoError(t, p.Set("host", newConn(t)))
	})

	t.Run("rejects a duplicate host", func(t *testing.T) {
		t.Parallel()

		p := connect.NewPool()
		require.NoError(t, p.Set("host", newConn(t)))

		err := p.Set("host", newConn(t))
		require.ErrorIs(t, err, connect.ErrDuplicateHost)
		require.Contains(t, err.Error(), "host")
	})

	t.Run("keeps the original connection after a duplicate is rejected", func(t *testing.T) {
		t.Parallel()

		p := connect.NewPool()
		original := newConn(t)
		require.NoError(t, p.Set("host", original))
		require.Error(t, p.Set("host", newConn(t)))

		cn, err := p.Conn("host")
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
		require.NoError(t, p.Set("host", conn))

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
		require.NoError(t, p.Set("host", conn))

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
		hosts   = 8
	)

	var wg sync.WaitGroup
	start := make(chan struct{})

	for i := range workers {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()

			host := fmt.Sprintf("host-%d", i%hosts)
			conn := newConn(t)

			<-start // line every goroutine up so the operations actually overlap

			// Multiple goroutines fight over each host: one wins, the rest get
			// ErrDuplicateHost. Close the connection we couldn't hand off so it
			// doesn't leak.
			if err := p.Set(host, conn); err != nil {
				require.ErrorIs(t, err, connect.ErrDuplicateHost)
				require.NoError(t, conn.Close())
			}

			// Conn may or may not find the host yet depending on scheduling;
			// both outcomes are valid, we only care that the read is race-free.
			if _, err := p.Conn(host); err != nil {
				require.ErrorIs(t, err, connect.ErrHostNotFound)
			}

			// Close racing against Set/Conn must also be safe. closeOnce means
			// only the first caller does the work; the rest are no-ops.
			require.NoError(t, p.Close())
		}(i)
	}

	close(start)
	wg.Wait()
}

func TestPoolGetOrSet(t *testing.T) {
	t.Parallel()

	insecureOpt := grpc.WithTransportCredentials(insecure.NewCredentials())

	t.Run("creates and registers a connection when host is absent", func(t *testing.T) {
		t.Parallel()

		p := connect.NewPool()
		t.Cleanup(func() { require.NoError(t, p.Close()) })

		conn, err := p.GetOrSet("host", insecureOpt)
		require.NoError(t, err)
		require.NotNil(t, conn)

		// The connection is registered, so a subsequent read returns the same one.
		cn, err := p.Conn("host")
		require.NoError(t, err)
		require.Same(t, conn, cn)
	})

	t.Run("returns the existing connection when host is present", func(t *testing.T) {
		t.Parallel()

		p := connect.NewPool()
		t.Cleanup(func() { require.NoError(t, p.Close()) })

		existing := newConn(t)
		require.NoError(t, p.Set("host", existing))

		conn, err := p.GetOrSet("host", insecureOpt)
		require.NoError(t, err)
		require.Same(t, existing, conn)
	})

	t.Run("returns an error when dialing fails", func(t *testing.T) {
		t.Parallel()

		p := connect.NewPool()
		t.Cleanup(func() { require.NoError(t, p.Close()) })

		// No transport credentials option, so grpc.NewClient fails synchronously.
		conn, err := p.GetOrSet("host")
		require.Nil(t, conn)
		require.ErrorContains(t, err, "failed to connect")
	})

	t.Run("hands every concurrent caller the same connection", func(t *testing.T) {
		t.Parallel()

		// Many goroutines race to create the same host. Only one dial can be
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

				conn, err := p.GetOrSet("host", insecureOpt)
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
