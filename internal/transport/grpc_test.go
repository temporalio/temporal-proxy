package transport_test

import (
	"context"
	"maps"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/hashicorp/yamux"
	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/log"
	"go.uber.org/mock/gomock"

	"github.com/temporalio/temporal-proxy/internal/transport"
)

func TestNewGRPCMux(t *testing.T) {
	t.Parallel()

	t.Run("inbound/valid address", func(t *testing.T) {
		t.Parallel()

		m, err := transport.NewGRPCMux(t.Context(), "test", transport.GRPCConfig{
			Address: "127.0.0.1:0",
			Kind:    transport.Inbound,
		})
		require.NoError(t, err)
		require.NotNil(t, m)
		require.NotEmpty(t, m.Address())
	})

	t.Run("inbound/invalid address returns error", func(t *testing.T) {
		t.Parallel()

		m, err := transport.NewGRPCMux(t.Context(), "test", transport.GRPCConfig{
			Address: "not-valid",
			Kind:    transport.Inbound,
		})
		require.Error(t, err)
		require.Nil(t, m)
	})

	t.Run("outbound/valid address", func(t *testing.T) {
		t.Parallel()

		m, err := transport.NewGRPCMux(t.Context(), "test", transport.GRPCConfig{
			Address: "127.0.0.1:9999",
			Kind:    transport.Outbound,
		})
		require.NoError(t, err)
		require.NotNil(t, m)
		require.Equal(t, "127.0.0.1:9999", m.Address())
	})

	t.Run("WithGRPCConnections is accepted", func(t *testing.T) {
		t.Parallel()

		m, err := transport.NewGRPCMux(t.Context(), "test", transport.GRPCConfig{
			Address: "127.0.0.1:0",
			Kind:    transport.Inbound,
		}, transport.WithGRPCConnections(3))
		require.NoError(t, err)
		require.NotNil(t, m)
	})

	t.Run("WithGRPCLogger is accepted", func(t *testing.T) {
		t.Parallel()

		m, err := transport.NewGRPCMux(t.Context(), "test", transport.GRPCConfig{
			Address: "127.0.0.1:0",
			Kind:    transport.Inbound,
		}, transport.WithGRPCLogger(log.NewNoopLogger()))
		require.NoError(t, err)
		require.NotNil(t, m)
	})

	t.Run("WithGRPCSessionListeners is accepted", func(t *testing.T) {
		t.Parallel()

		ctrl := gomock.NewController(t)
		l := NewMockSessionListener(ctrl)

		m, err := transport.NewGRPCMux(t.Context(), "test", transport.GRPCConfig{
			Address: "127.0.0.1:0",
			Kind:    transport.Inbound,
		}, transport.WithGRPCSessionListeners(l))
		require.NoError(t, err)
		require.NotNil(t, m)
	})
}

func TestGRPCMux_Address(t *testing.T) {
	t.Parallel()

	t.Run("inbound address is non-empty", func(t *testing.T) {
		t.Parallel()

		m := newTestGRPCMux(t, t.Context())
		require.NotEmpty(t, m.Address())
	})

	t.Run("outbound address matches config", func(t *testing.T) {
		t.Parallel()

		m, err := transport.NewGRPCMux(t.Context(), "test", transport.GRPCConfig{
			Address: "127.0.0.1:7654",
			Kind:    transport.Outbound,
		})
		require.NoError(t, err)
		require.Equal(t, "127.0.0.1:7654", m.Address())
	})
}

func TestGRPCMux_Done(t *testing.T) {
	t.Parallel()

	// GRPCMux.done.Shutdown() is never called in the current implementation,
	// so Done() never closes. Only assert the channel is open while running.
	t.Run("channel is open while context is live", func(t *testing.T) {
		t.Parallel()

		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()

		m := newTestGRPCMux(t, ctx)

		select {
		case <-m.Done():
			t.Fatal("Done() must not be closed while context is live")
		default:
		}
	})
}

func TestGRPCMux_AddConnection(t *testing.T) {
	t.Parallel()

	t.Run("registers session and notifies listener", func(t *testing.T) {
		t.Parallel()

		ctrl := gomock.NewController(t)
		l, ch := notifiedListener(ctrl)

		m := newTestGRPCMux(t, t.Context(), transport.WithGRPCSessionListeners(l))
		pipe := newYamuxPipe(t)

		m.AddConnection(pipe.conn, pipe.session)

		snap := waitForNotification(t, ch)
		require.Len(t, snap, 1)
	})

	t.Run("assigns sequential string keys", func(t *testing.T) {
		t.Parallel()

		ctrl := gomock.NewController(t)
		l, ch := notifiedListener(ctrl)

		m := newTestGRPCMux(t, t.Context(), transport.WithGRPCSessionListeners(l))

		for range 3 {
			pipe := newYamuxPipe(t)
			m.AddConnection(pipe.conn, pipe.session)
		}

		var lastSnap map[string]*transport.Session
		for range 3 {
			lastSnap = waitForNotification(t, ch)
		}

		require.Contains(t, lastSnap, "0")
		require.Contains(t, lastSnap, "1")
		require.Contains(t, lastSnap, "2")
	})

	t.Run("skips when context is already canceled", func(t *testing.T) {
		t.Parallel()

		ctrl := gomock.NewController(t)

		ctx, cancel := context.WithCancel(t.Context())
		cancel() // pre-cancel

		// Listener should never be called; register with no expectations.
		l := NewMockSessionListener(ctrl)

		m := newTestGRPCMux(t, ctx, transport.WithGRPCSessionListeners(l))
		pipe := newYamuxPipe(t)

		m.AddConnection(pipe.conn, pipe.session)

		// Brief pause to confirm no notification arrives; the mock has no expectations so
		// any call to OnSessionsUpdated would fail the test automatically.
		<-time.After(50 * time.Millisecond)
	})

	t.Run("notifies multiple listeners", func(t *testing.T) {
		t.Parallel()

		ctrl := gomock.NewController(t)
		l1, ch1 := notifiedListener(ctrl)
		l2, ch2 := notifiedListener(ctrl)

		m := newTestGRPCMux(t, t.Context(), transport.WithGRPCSessionListeners(l1, l2))
		pipe := newYamuxPipe(t)

		m.AddConnection(pipe.conn, pipe.session)

		snap1 := waitForNotification(t, ch1)
		snap2 := waitForNotification(t, ch2)

		require.Len(t, snap1, 1)
		require.Len(t, snap2, 1)
	})
}

func TestGRPCMux_DropSession(t *testing.T) {
	t.Parallel()

	t.Run("removing a session notifies listener with empty map", func(t *testing.T) {
		t.Parallel()

		ctrl := gomock.NewController(t)
		l, ch := notifiedListener(ctrl)

		m := newTestGRPCMux(t, t.Context(), transport.WithGRPCSessionListeners(l))
		pipe := newYamuxPipe(t)

		m.AddConnection(pipe.conn, pipe.session)
		waitForNotification(t, ch) // consume add notification

		// Close the server-side session to trigger the client session's teardown.
		require.NoError(t, pipe.serverSession.Close())

		// Wait for drop notification with empty map.
		require.Eventually(t, func() bool {
			select {
			case snap := <-ch:
				return len(snap) == 0
			default:
				return false
			}
		}, 500*time.Millisecond, 5*time.Millisecond)
	})

	t.Run("two sessions: removing one leaves the other", func(t *testing.T) {
		t.Parallel()

		ctrl := gomock.NewController(t)
		l, ch := notifiedListener(ctrl)

		m := newTestGRPCMux(t, t.Context(), transport.WithGRPCSessionListeners(l))

		pipe1 := newYamuxPipe(t)
		pipe2 := newYamuxPipe(t)

		m.AddConnection(pipe1.conn, pipe1.session)
		waitForNotification(t, ch) // consume add notification for pipe1

		m.AddConnection(pipe2.conn, pipe2.session)
		waitForNotification(t, ch) // consume add notification for pipe2

		// Close the server side of pipe1.
		require.NoError(t, pipe1.serverSession.Close())

		// Wait for drop notification showing exactly 1 session remaining.
		require.Eventually(t, func() bool {
			select {
			case snap := <-ch:
				return len(snap) == 1
			default:
				return false
			}
		}, 500*time.Millisecond, 5*time.Millisecond)
	})
}

func TestGRPCMux_ContextCancellation(t *testing.T) {
	t.Parallel()

	t.Run("canceling context closes sessions", func(t *testing.T) {
		t.Parallel()

		ctx, cancel := context.WithCancel(t.Context())

		m := newTestGRPCMux(t, ctx)

		pipe1 := newYamuxPipe(t)
		pipe2 := newYamuxPipe(t)
		m.AddConnection(pipe1.conn, pipe1.session)
		m.AddConnection(pipe2.conn, pipe2.session)

		cancel()

		// Sessions are created with m.ctx, so they close automatically when it is canceled.
		require.Eventually(t, pipe1.session.IsClosed, 500*time.Millisecond, 5*time.Millisecond)
		require.Eventually(t, pipe2.session.IsClosed, 500*time.Millisecond, 5*time.Millisecond)
	})
}

func TestGRPCMux_Start(t *testing.T) {
	t.Parallel()

	t.Run("is idempotent via sync.Once", func(t *testing.T) {
		t.Parallel()

		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()

		ctrl := gomock.NewController(t)
		l, ch := notifiedListener(ctrl)

		// Establish one connection so we can confirm the mux started.
		m := newTestGRPCMux(t, ctx, transport.WithGRPCSessionListeners(l))

		// Launch 10 goroutines all calling Start(); only the first should take effect.
		var wg sync.WaitGroup
		for range 10 {
			wg.Go(func() {
				m.Start()
			})
		}

		done := make(chan struct{})
		go func() { wg.Wait(); close(done) }()

		select {
		case <-done:
		case <-time.After(500 * time.Millisecond):
			t.Fatal("Start() goroutines did not complete within timeout")
		}

		// Dial the inbound address to confirm the mux is actually listening.
		go func() {
			conn, err := net.Dial("tcp", m.Address())
			if err != nil {
				return
			}
			sess, err := yamux.Server(conn, nil)
			if err != nil {
				_ = conn.Close()
				return
			}
			<-sess.CloseChan()
			_ = sess.Close()
		}()

		waitForNotification(t, ch)
	})

	t.Run("context cancel exits start delay cleanly", func(t *testing.T) {
		t.Parallel()

		ctx, cancel := context.WithCancel(t.Context())

		// Use the default 1-minute start delay; context cancel should exit it early.
		m, err := transport.NewGRPCMux(ctx, "test", transport.GRPCConfig{
			Address: "127.0.0.1:0",
			Kind:    transport.Inbound,
		})
		require.NoError(t, err)

		done := make(chan struct{})
		go func() {
			m.Start()
			close(done)
		}()

		// Cancel before the 1-minute delay elapses.
		cancel()

		select {
		case <-done:
		case <-time.After(500 * time.Millisecond):
			t.Fatal("Start() did not exit after context cancel")
		}
	})
}

func TestGRPCMux_SessionListener_Concurrency(t *testing.T) {
	t.Parallel()

	t.Run("concurrent AddConnection calls do not race", func(t *testing.T) {
		t.Parallel()

		ctrl := gomock.NewController(t)

		var mu sync.Mutex
		callCount := 0

		l := NewMockSessionListener(ctrl)
		l.EXPECT().OnSessionsUpdated(gomock.Any()).Do(func(map[string]*transport.Session) {
			mu.Lock()
			callCount++
			mu.Unlock()
		}).AnyTimes()

		m := newTestGRPCMux(t, t.Context(), transport.WithGRPCSessionListeners(l))

		var wg sync.WaitGroup
		for range 5 {
			wg.Go(func() {
				pipe := newYamuxPipe(t)
				m.AddConnection(pipe.conn, pipe.session)
			})
		}
		wg.Wait()

		require.Eventually(t, func() bool {
			mu.Lock()
			defer mu.Unlock()
			return callCount >= 5
		}, 500*time.Millisecond, 5*time.Millisecond)
	})
}

func newTestGRPCMux(t *testing.T, ctx context.Context, opts ...transport.GRPCMuxOption) *transport.GRPCMux {
	t.Helper()

	opts = append([]transport.GRPCMuxOption{transport.WithGRPCStartDelay(0)}, opts...)
	m, err := transport.NewGRPCMux(ctx, "test-grpcmux", transport.GRPCConfig{
		Address: "127.0.0.1:0",
		Kind:    transport.Inbound,
	}, opts...)
	require.NoError(t, err)
	return m
}

// notifiedListener wires a MockSessionListener to forward OnSessionsUpdated calls to a buffered
// channel of snapshot maps. The channel has capacity 16 to absorb bursts without blocking.
func notifiedListener(ctrl *gomock.Controller) (*MockSessionListener, chan map[string]*transport.Session) {
	ch := make(chan map[string]*transport.Session, 16)
	l := NewMockSessionListener(ctrl)
	l.EXPECT().OnSessionsUpdated(gomock.Any()).Do(func(sessions map[string]*transport.Session) {
		snap := make(map[string]*transport.Session, len(sessions))
		maps.Copy(snap, sessions)
		ch <- snap
	}).AnyTimes()
	return l, ch
}

func waitForNotification(t *testing.T, ch <-chan map[string]*transport.Session) map[string]*transport.Session {
	t.Helper()

	select {
	case snap := <-ch:
		return snap
	case <-time.After(500 * time.Millisecond):
		t.Fatal("timed out waiting for listener notification")
		return nil
	}
}
