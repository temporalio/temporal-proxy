package transport_test

import (
	"context"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hashicorp/yamux"
	"github.com/stretchr/testify/require"

	"github.com/temporalio/temporal-proxy/internal/transport"
)

type yamuxPipe struct {
	conn          net.Conn
	session       *yamux.Session
	serverConn    net.Conn
	serverSession *yamux.Session
}

func TestNewSession(t *testing.T) {
	t.Parallel()

	t.Run("initial state", func(t *testing.T) {
		t.Parallel()

		pipe := newYamuxPipe(t)
		s := transport.NewSession(t.Context(), "test-id", pipe.conn, pipe.session)
		defer func() { _ = s.Close() }()

		info := s.State()
		require.Equal(t, transport.Connected, info.State)
		require.NoError(t, info.Err)
		require.False(t, s.IsClosed())
		require.Contains(t, s.String(), "id:test-id")

		select {
		case <-s.Done():
			t.Fatal("Done channel should not be closed on a live session")
		default:
		}
	})

	t.Run("builder is called with correct arguments", func(t *testing.T) {
		t.Parallel()

		pipe := newYamuxPipe(t)

		var capturedID string
		var capturedSession *yamux.Session
		builder := transport.SessionBuilder(func(_ context.Context, id string, sess *yamux.Session) {
			capturedID = id
			capturedSession = sess
		})

		s := transport.NewSession(
			t.Context(),
			"builder-id",
			pipe.conn,
			pipe.session,
			transport.WithSessionBuilder(builder),
		)
		defer func() { _ = s.Close() }()

		require.Equal(t, "builder-id", capturedID)
		require.Equal(t, pipe.session, capturedSession)
	})

	t.Run("multiple builders are all called", func(t *testing.T) {
		t.Parallel()

		pipe := newYamuxPipe(t)

		var calls atomic.Int32
		inc := transport.SessionBuilder(func(context.Context, string, *yamux.Session) { calls.Add(1) })

		s := transport.NewSession(
			t.Context(),
			"test-id",
			pipe.conn,
			pipe.session,
			transport.WithSessionBuilder(inc, inc),
		)
		defer func() { _ = s.Close() }()

		require.Equal(t, int32(2), calls.Load())
	})
}

func TestSessionClose(t *testing.T) {
	t.Parallel()

	t.Run("Close returns nil", func(t *testing.T) {
		t.Parallel()

		pipe := newYamuxPipe(t)
		s := transport.NewSession(t.Context(), "test-id", pipe.conn, pipe.session)

		require.NoError(t, s.Close())
	})

	t.Run("IsClosed returns true after Close", func(t *testing.T) {
		t.Parallel()

		pipe := newYamuxPipe(t)
		s := transport.NewSession(t.Context(), "test-id", pipe.conn, pipe.session)

		require.NoError(t, s.Close())
		require.True(t, s.IsClosed())
	})

	t.Run("Done channel closes after Close", func(t *testing.T) {
		t.Parallel()

		pipe := newYamuxPipe(t)
		s := transport.NewSession(t.Context(), "test-id", pipe.conn, pipe.session)

		require.NoError(t, s.Close())
		require.Eventually(t, func() bool {
			select {
			case <-s.Done():
				return true
			default:
				return false
			}
		}, 500*time.Millisecond, 5*time.Millisecond)
	})

	t.Run("state transitions to Closed", func(t *testing.T) {
		t.Parallel()

		pipe := newYamuxPipe(t)
		s := transport.NewSession(t.Context(), "test-id", pipe.conn, pipe.session)

		require.NoError(t, s.Close())
		require.Eventually(t, func() bool {
			return s.State().State == transport.Closed
		}, 500*time.Millisecond, 5*time.Millisecond)
	})

	t.Run("teardown is called", func(t *testing.T) {
		t.Parallel()

		var called atomic.Bool
		pipe := newYamuxPipe(t)

		s := transport.NewSession(
			t.Context(),
			"test-id",
			pipe.conn,
			pipe.session,
			transport.WithSessionTeardown(func() { called.Store(true) }),
		)

		require.NoError(t, s.Close())
		require.Eventually(t, called.Load, 500*time.Millisecond, 5*time.Millisecond)
	})

	t.Run("multiple Close calls do not panic", func(t *testing.T) {
		t.Parallel()

		pipe := newYamuxPipe(t)
		s := transport.NewSession(t.Context(), "test-id", pipe.conn, pipe.session)

		require.NotPanics(t, func() {
			_ = s.Close()
			_ = s.Close()
			_ = s.Close()
		})
	})
}

func TestSessionOpen(t *testing.T) {
	t.Parallel()

	t.Run("Open succeeds on live session", func(t *testing.T) {
		t.Parallel()

		pipe := newYamuxPipe(t)
		s := transport.NewSession(t.Context(), "test-id", pipe.conn, pipe.session)
		defer func() { _ = s.Close() }()

		conn, err := s.Open()
		require.NoError(t, err)
		require.NotNil(t, conn)
		require.NoError(t, conn.Close())
	})

	t.Run("Open returns ErrSessionClosed after Close", func(t *testing.T) {
		t.Parallel()

		pipe := newYamuxPipe(t)
		s := transport.NewSession(t.Context(), "test-id", pipe.conn, pipe.session)
		require.NoError(t, s.Close())

		conn, err := s.Open()
		require.ErrorIs(t, err, transport.ErrSessionClosed)
		require.Nil(t, conn)
	})

	t.Run("Open returns ErrSessionClosed when parent context canceled", func(t *testing.T) {
		t.Parallel()

		ctx, cancel := context.WithCancel(t.Context())
		pipe := newYamuxPipe(t)
		s := transport.NewSession(ctx, "test-id", pipe.conn, pipe.session)

		cancel()
		require.Eventually(t, s.IsClosed, 500*time.Millisecond, 5*time.Millisecond)

		conn, err := s.Open()
		require.ErrorIs(t, err, transport.ErrSessionClosed)
		require.Nil(t, conn)
	})
}

func TestSessionAccept(t *testing.T) {
	t.Parallel()

	t.Run("Accept returns ErrSessionClosed after Close", func(t *testing.T) {
		t.Parallel()

		pipe := newYamuxPipe(t)
		s := transport.NewSession(t.Context(), "test-id", pipe.conn, pipe.session)
		require.NoError(t, s.Close())

		conn, err := s.Accept()
		require.ErrorIs(t, err, transport.ErrSessionClosed)
		require.Nil(t, conn)
	})

	t.Run("Accept receives stream opened by peer", func(t *testing.T) {
		t.Parallel()

		pipe := newYamuxPipe(t)
		s := transport.NewSession(t.Context(), "test-id", pipe.serverConn, pipe.serverSession)
		defer func() { _ = s.Close() }()

		connCh := make(chan net.Conn, 1)
		errCh := make(chan error, 1)
		go func() {
			conn, err := s.Accept()
			if err != nil {
				errCh <- err
				return
			}
			connCh <- conn
		}()

		stream, err := pipe.session.Open()
		require.NoError(t, err)
		defer func() { _ = stream.Close() }()

		select {
		case conn := <-connCh:
			require.NotNil(t, conn)
			_ = conn.Close()
		case err := <-errCh:
			require.NoError(t, err)
		case <-time.After(500 * time.Millisecond):
			t.Fatal("Accept did not return within timeout")
		}
	})
}

func TestSessionAddr(t *testing.T) {
	t.Parallel()

	pipe := newYamuxPipe(t)
	s := transport.NewSession(t.Context(), "test-id", pipe.conn, pipe.session)
	defer func() { _ = s.Close() }()

	require.NotNil(t, s.Addr())
}

func TestSessionState(t *testing.T) {
	t.Parallel()

	t.Run("State returns Connected initially", func(t *testing.T) {
		t.Parallel()

		pipe := newYamuxPipe(t)
		s := transport.NewSession(t.Context(), "test-id", pipe.conn, pipe.session)
		defer func() { _ = s.Close() }()

		require.Equal(t, transport.Connected, s.State().State)
	})

	t.Run("State is Closed after Close", func(t *testing.T) {
		t.Parallel()

		pipe := newYamuxPipe(t)
		s := transport.NewSession(t.Context(), "test-id", pipe.conn, pipe.session)
		require.NoError(t, s.Close())

		require.Eventually(t, func() bool {
			return s.State().State == transport.Closed
		}, 500*time.Millisecond, 5*time.Millisecond)
	})

	t.Run("State transitions to Closed when remote yamux closes", func(t *testing.T) {
		t.Parallel()

		pipe := newYamuxPipe(t)
		s := transport.NewSession(t.Context(), "test-id", pipe.serverConn, pipe.serverSession)
		defer func() { _ = s.Close() }()

		require.NoError(t, pipe.session.Close())
		require.Eventually(t, func() bool {
			return s.State().State == transport.Closed
		}, 500*time.Millisecond, 5*time.Millisecond)
	})
}

func TestSessionContextPropagation(t *testing.T) {
	t.Parallel()

	t.Run("parent context cancel closes session", func(t *testing.T) {
		t.Parallel()

		ctx, cancel := context.WithCancel(t.Context())
		pipe := newYamuxPipe(t)
		s := transport.NewSession(ctx, "test-id", pipe.conn, pipe.session)

		cancel()
		require.Eventually(t, s.IsClosed, 500*time.Millisecond, 5*time.Millisecond)
	})

	t.Run("parent context cancel calls teardown", func(t *testing.T) {
		t.Parallel()

		ctx, cancel := context.WithCancel(t.Context())
		pipe := newYamuxPipe(t)

		var called atomic.Bool
		transport.NewSession(
			ctx,
			"test-id",
			pipe.conn,
			pipe.session,
			transport.WithSessionTeardown(func() { called.Store(true) }),
		)

		cancel()
		require.Eventually(t, called.Load, 500*time.Millisecond, 5*time.Millisecond)
	})
}

func TestSessionTeardownOnRemoteClose(t *testing.T) {
	t.Parallel()

	t.Run("teardown called when remote yamux closes", func(t *testing.T) {
		t.Parallel()

		pipe := newYamuxPipe(t)

		var called atomic.Bool
		s := transport.NewSession(
			t.Context(),
			"test-id",
			pipe.serverConn,
			pipe.serverSession,
			transport.WithSessionTeardown(func() { called.Store(true) }),
		)

		require.NoError(t, pipe.session.Close())
		require.Eventually(t, called.Load, 500*time.Millisecond, 5*time.Millisecond)
		require.Eventually(t, func() bool {
			return s.State().State == transport.Closed
		}, 500*time.Millisecond, 5*time.Millisecond)
	})
}

func newYamuxPipe(t *testing.T) *yamuxPipe {
	t.Helper()

	clientConn, serverConn := net.Pipe()
	clientSession, err := yamux.Client(clientConn, nil)
	require.NoError(t, err)

	serverSession, err := yamux.Server(serverConn, nil)
	require.NoError(t, err)

	t.Cleanup(func() {
		_ = clientSession.Close()
		_ = serverSession.Close()
		_ = clientConn.Close()
		_ = serverConn.Close()
	})

	return &yamuxPipe{
		conn:          clientConn,
		session:       clientSession,
		serverConn:    serverConn,
		serverSession: serverSession,
	}
}
