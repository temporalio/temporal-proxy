package transport_test

//go:generate go tool -modfile ../../dev/tools.mod mockgen -destination mocks_test.go -package transport_test -typed . ConnectionFactory,ConnectionManager,SessionFactory,SessionListener

import (
	"context"
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hashicorp/yamux"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/temporalio/temporal-proxy/internal/transport"
)

// closeTrackingConn wraps net.Conn and counts Close() calls.
type closeTrackingConn struct {
	net.Conn
	closed *atomic.Int32
}

func TestNewInboundMux(t *testing.T) {
	t.Parallel()

	t.Run("returns a valid Mux", func(t *testing.T) {
		t.Parallel()

		ctrl := gomock.NewController(t)
		mgr := NewMockConnectionManager(ctrl)

		mux, err := transport.NewInboundMux(t.Context(), "test", "127.0.0.1:0", 1, mgr, transport.FactoryOptions{})
		require.NoError(t, err)
		require.NotNil(t, mux)
		require.NotEmpty(t, mux.Address())
	})

	t.Run("returns error for invalid address", func(t *testing.T) {
		t.Parallel()

		ctrl := gomock.NewController(t)
		mgr := NewMockConnectionManager(ctrl)

		_, err := transport.NewInboundMux(t.Context(), "test", "invalid-addr", 1, mgr, transport.FactoryOptions{})
		require.Error(t, err)
	})

	t.Run("accepts connection from real dialer", func(t *testing.T) {
		t.Parallel()

		ctx, cancel := context.WithCancel(t.Context())
		ctrl := gomock.NewController(t)
		mgr := NewMockConnectionManager(ctrl)

		added := make(chan struct{})
		mgr.EXPECT().AddConnection(gomock.Any(), gomock.Any()).Do(func(net.Conn, *yamux.Session) {
			close(added)
		})

		mux, err := transport.NewInboundMux(ctx, "test", "127.0.0.1:0", 1, mgr, transport.FactoryOptions{})
		require.NoError(t, err)

		mux.Start()

		// Dial to the inbound mux and run a yamux server so the ping succeeds.
		go func() {
			conn, err := net.Dial("tcp", mux.Address())
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

		select {
		case <-added:
		case <-time.After(500 * time.Millisecond):
			t.Fatal("expected AddConnection to be called")
		}

		cancel()
		mux.Wait()
	})
}

func TestNewMux(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)

	t.Run("returns a valid Mux", func(t *testing.T) {
		t.Parallel()

		cf := NewMockConnectionFactory(ctrl)
		sf := NewMockSessionFactory(ctrl)
		mgr := NewMockConnectionManager(ctrl)

		mux, err := transport.NewMux(
			t.Context(),
			"test",
			1,
			transport.WithConnectionFactory(cf),
			transport.WithSessionFactory(sf),
			transport.WithConnectionManager(mgr),
		)

		require.NoError(t, err)
		require.NotNil(t, mux)
	})

	t.Run("enforces minimum of 1 connection", func(t *testing.T) {
		t.Parallel()

		cf := NewMockConnectionFactory(ctrl)
		sf := NewMockSessionFactory(ctrl)
		mgr := NewMockConnectionManager(ctrl)

		for _, i := range []int{-1, 0} {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()

			doneCh, closeDone := makeDoneCh(t)
			clientConn, _ := newYamuxServerPair(t)
			added := make(chan struct{}, 1)

			cf.EXPECT().Done().Return(doneCh).AnyTimes()
			cf.EXPECT().NewConnection().Return(clientConn, nil)
			sf.EXPECT().NewSession(gomock.Any()).DoAndReturn(func(conn net.Conn) (*yamux.Session, error) {
				return yamuxClientSession(t, conn), nil
			})
			mgr.EXPECT().AddConnection(gomock.Any(), gomock.Any()).Do(func(net.Conn, *yamux.Session) {
				added <- struct{}{}
			})

			mux := newTestMux(t, ctx, i, cf, sf, mgr)
			mux.Start()

			// numConns<=0 is coerced to 1 — exactly 1 connection should be added.
			select {
			case <-added:
			case <-time.After(500 * time.Millisecond):
				t.Fatal("expected exactly 1 connection to be added")
			}

			cancel()
			closeDone()
			mux.Wait()
		}
	})
}

func TestNewOutboundMux(t *testing.T) {
	t.Parallel()

	t.Run("returns a valid Mux", func(t *testing.T) {
		t.Parallel()

		ctrl := gomock.NewController(t)
		mgr := NewMockConnectionManager(ctrl)

		mux, err := transport.NewOutboundMux(t.Context(), "test", "127.0.0.1:1234", 1, mgr, transport.FactoryOptions{
			RetryPolicy: fastRetryPolicy,
		})

		require.NoError(t, err)
		require.NotNil(t, mux)
		require.Equal(t, "127.0.0.1:1234", mux.Address())
	})

	t.Run("establishes connection to real listener", func(t *testing.T) {
		t.Parallel()

		ln, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		t.Cleanup(func() { _ = ln.Close() })

		// Serve yamux on the listener side.
		go func() {
			for {
				conn, err := ln.Accept()
				if err != nil {
					return
				}
				go func() {
					sess, err := yamux.Server(conn, nil)
					if err != nil {
						_ = conn.Close()
						return
					}
					<-sess.CloseChan()
					_ = sess.Close()
				}()
			}
		}()

		ctx, cancel := context.WithCancel(t.Context())
		ctrl := gomock.NewController(t)
		mgr := NewMockConnectionManager(ctrl)

		added := make(chan struct{})
		mgr.EXPECT().AddConnection(gomock.Any(), gomock.Any()).Do(func(net.Conn, *yamux.Session) {
			close(added)
		})

		mux, err := transport.NewOutboundMux(ctx, "test", ln.Addr().String(), 1, mgr, transport.FactoryOptions{
			RetryPolicy: fastRetryPolicy,
		})
		require.NoError(t, err)

		mux.Start()

		select {
		case <-added:
		case <-time.After(500 * time.Millisecond):
			t.Fatal("expected AddConnection to be called")
		}

		cancel()
		mux.Wait()
	})
}

func TestMux_Address(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	cf := NewMockConnectionFactory(ctrl)
	cf.EXPECT().Address().Return("127.0.0.1:9999")

	mux, err := transport.NewMux(t.Context(), "test", 1, transport.WithConnectionFactory(cf))
	require.NoError(t, err)
	require.Equal(t, "127.0.0.1:9999", mux.Address())
}

func TestMux_Wait(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)

	t.Run("Wait before Start returns without blocking", func(t *testing.T) {
		t.Parallel()

		cf := NewMockConnectionFactory(ctrl)
		sf := NewMockSessionFactory(ctrl)
		mgr := NewMockConnectionManager(ctrl)

		mux := newTestMux(t, t.Context(), 1, cf, sf, mgr)
		done := make(chan struct{})
		go func() { mux.Wait(); close(done) }()

		select {
		case <-done:
		case <-time.After(200 * time.Millisecond):
			t.Fatal("Wait() blocked when Start() was never called")
		}
	})

	t.Run("Wait returns after context cancel and factory done", func(t *testing.T) {
		t.Parallel()

		cf := NewMockConnectionFactory(ctrl)
		sf := NewMockSessionFactory(ctrl)
		mgr := NewMockConnectionManager(ctrl)

		ctx, cancel := context.WithCancel(t.Context())
		doneCh, closeDone := makeDoneCh(t)

		cf.EXPECT().Done().Return(doneCh).AnyTimes()
		cf.EXPECT().NewConnection().DoAndReturn(func() (net.Conn, error) {
			<-ctx.Done()
			return nil, ctx.Err()
		}).AnyTimes()

		mux := newTestMux(t, ctx, 1, cf, sf, mgr)
		mux.Start()

		cancel()
		closeDone()

		done := make(chan struct{})
		go func() { mux.Wait(); close(done) }()

		select {
		case <-done:
		case <-time.After(500 * time.Millisecond):
			t.Fatal("Wait() did not return after shutdown")
		}
	})
}

func TestMux_Done(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)

	t.Run("Done channel is open while running", func(t *testing.T) {
		t.Parallel()

		cf := NewMockConnectionFactory(ctrl)
		sf := NewMockSessionFactory(ctrl)
		mgr := NewMockConnectionManager(ctrl)

		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()

		doneCh, closeDone := makeDoneCh(t)
		cf.EXPECT().Done().Return(doneCh).AnyTimes()
		cf.EXPECT().NewConnection().DoAndReturn(func() (net.Conn, error) {
			<-ctx.Done()
			return nil, ctx.Err()
		}).AnyTimes()

		mux := newTestMux(t, ctx, 1, cf, sf, mgr)
		mux.Start()

		select {
		case <-mux.Done():
			t.Fatal("Done() should not be closed while mux is running")
		default:
		}

		cancel()
		closeDone()
	})

	t.Run("Done channel closes after shutdown", func(t *testing.T) {
		t.Parallel()

		cf := NewMockConnectionFactory(ctrl)
		sf := NewMockSessionFactory(ctrl)
		mgr := NewMockConnectionManager(ctrl)

		ctx, cancel := context.WithCancel(t.Context())
		doneCh, closeDone := makeDoneCh(t)

		cf.EXPECT().Done().Return(doneCh).AnyTimes()
		cf.EXPECT().NewConnection().DoAndReturn(func() (net.Conn, error) {
			<-ctx.Done()
			return nil, ctx.Err()
		}).AnyTimes()

		mux := newTestMux(t, ctx, 1, cf, sf, mgr)
		mux.Start()

		cancel()
		closeDone()
		mux.Wait()

		select {
		case <-mux.Done():
		default:
			t.Fatal("Done() should be closed after Wait() returns")
		}
	})
}

func TestMux_Start(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)

	t.Run("Start is idempotent via sync.Once", func(t *testing.T) {
		t.Parallel()

		cf := NewMockConnectionFactory(ctrl)
		sf := NewMockSessionFactory(ctrl)
		mgr := NewMockConnectionManager(ctrl)

		ctx, cancel := context.WithCancel(t.Context())
		doneCh, closeDone := makeDoneCh(t)

		conn, _ := newYamuxServerPair(t)
		added := make(chan struct{}, 1)

		cf.EXPECT().Done().Return(doneCh).AnyTimes()
		cf.EXPECT().NewConnection().Return(conn, nil)
		sf.EXPECT().NewSession(gomock.Any()).DoAndReturn(func(conn net.Conn) (*yamux.Session, error) {
			return yamuxClientSession(t, conn), nil
		})
		mgr.EXPECT().AddConnection(gomock.Any(), gomock.Any()).Do(func(net.Conn, *yamux.Session) {
			added <- struct{}{}
		})

		mux := newTestMux(t, ctx, 1, cf, sf, mgr)

		var wg sync.WaitGroup
		for range 10 {
			wg.Go(func() { mux.Start() })
		}
		wg.Wait()

		select {
		case <-added:
		case <-time.After(500 * time.Millisecond):
			t.Fatal("expected connection to be added")
		}

		// Brief pause to give the goroutine time to attempt a second connection, which
		// would violate the mock's Times(1) expectation and catch a sync.Once regression.
		time.Sleep(20 * time.Millisecond)

		cancel()
		closeDone()
		mux.Wait()
	})
}

func TestMux_ConnectionRetry(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)

	t.Run("retries on NewConnection failure then succeeds", func(t *testing.T) {
		t.Parallel()

		cf := NewMockConnectionFactory(ctrl)
		sf := NewMockSessionFactory(ctrl)
		mgr := NewMockConnectionManager(ctrl)

		ctx, cancel := context.WithCancel(t.Context())
		doneCh, closeDone := makeDoneCh(t)

		conn, _ := newYamuxServerPair(t)
		added := make(chan struct{}, 1)

		cf.EXPECT().Done().Return(doneCh)

		gomock.InOrder(
			cf.EXPECT().NewConnection().Return(nil, errors.New("dial fail")),
			cf.EXPECT().NewConnection().Return(nil, errors.New("dial fail")),
			cf.EXPECT().NewConnection().Return(conn, nil),
		)

		sf.EXPECT().NewSession(gomock.Any()).DoAndReturn(func(conn net.Conn) (*yamux.Session, error) {
			return yamuxClientSession(t, conn), nil
		})

		mgr.EXPECT().AddConnection(gomock.Any(), gomock.Any()).Do(func(net.Conn, *yamux.Session) {
			added <- struct{}{}
		})

		mux := newTestMux(t, ctx, 1, cf, sf, mgr)
		mux.Start()

		select {
		case <-added:
		case <-time.After(500 * time.Millisecond):
			t.Fatal("expected connection after retries")
		}

		cancel()
		closeDone()
		mux.Wait()
	})

	t.Run("retries on NewSession failure then succeeds", func(t *testing.T) {
		t.Parallel()

		ctx, cancel := context.WithCancel(t.Context())
		cf := NewMockConnectionFactory(ctrl)
		sf := NewMockSessionFactory(ctrl)
		mgr := NewMockConnectionManager(ctrl)

		doneCh, closeDone := makeDoneCh(t)
		added := make(chan struct{}, 1)
		var closedConns atomic.Int32

		cf.EXPECT().Done().Return(doneCh).AnyTimes()
		cf.EXPECT().NewConnection().DoAndReturn(func() (net.Conn, error) {
			clientConn, serverConn := net.Pipe()
			go func() {
				srv, err := yamux.Server(serverConn, nil)
				if err != nil {
					return
				}
				<-srv.CloseChan()
				_ = srv.Close()
				_ = serverConn.Close()
			}()

			return &closeTrackingConn{Conn: clientConn, closed: &closedConns}, nil
		}).AnyTimes()

		gomock.InOrder(
			sf.EXPECT().NewSession(gomock.Any()).Return(nil, errors.New("session fail")),
			sf.EXPECT().NewSession(gomock.Any()).Return(nil, errors.New("session fail")),
			sf.EXPECT().NewSession(gomock.Any()).DoAndReturn(func(conn net.Conn) (*yamux.Session, error) {
				return yamuxClientSession(t, conn), nil
			}),
		)

		mgr.EXPECT().AddConnection(gomock.Any(), gomock.Any()).Do(func(net.Conn, *yamux.Session) {
			added <- struct{}{}
		})

		mux := newTestMux(t, ctx, 1, cf, sf, mgr)
		mux.Start()

		select {
		case <-added:
		case <-time.After(500 * time.Millisecond):
			t.Fatal("expected connection after session retries")
		}

		require.GreaterOrEqual(t, int(closedConns.Load()), 2)

		cancel()
		closeDone()
		mux.Wait()
	})

	t.Run("context cancel during NewConnection error stops loop", func(t *testing.T) {
		t.Parallel()

		ctx, cancel := context.WithCancel(t.Context())
		cf := NewMockConnectionFactory(ctrl)
		sf := NewMockSessionFactory(ctrl)
		mgr := NewMockConnectionManager(ctrl)

		doneCh, closeDone := makeDoneCh(t)
		cf.EXPECT().Done().Return(doneCh).AnyTimes()
		cf.EXPECT().NewConnection().Return(nil, errors.New("always fail")).AnyTimes()

		mux := newTestMux(t, ctx, 1, cf, sf, mgr)
		mux.Start()

		cancel()
		closeDone()

		done := make(chan struct{})
		go func() { mux.Wait(); close(done) }()

		select {
		case <-done:
		case <-time.After(500 * time.Millisecond):
			t.Fatal("Wait() did not return after context cancel")
		}
	})
}

func TestMux_PingBehavior(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)

	t.Run("successful ping leads to AddConnection", func(t *testing.T) {
		t.Parallel()

		ctx, cancel := context.WithCancel(t.Context())
		cf := NewMockConnectionFactory(ctrl)
		sf := NewMockSessionFactory(ctrl)
		mgr := NewMockConnectionManager(ctrl)

		doneCh, closeDone := makeDoneCh(t)
		clientConn, _ := newYamuxServerPair(t)
		added := make(chan struct{}, 1)

		cf.EXPECT().Done().Return(doneCh).AnyTimes()
		cf.EXPECT().NewConnection().Return(clientConn, nil)
		sf.EXPECT().NewSession(gomock.Any()).DoAndReturn(func(conn net.Conn) (*yamux.Session, error) {
			return yamuxClientSession(t, conn), nil
		})

		mgr.EXPECT().AddConnection(gomock.Any(), gomock.Any()).Do(func(conn net.Conn, sess *yamux.Session) {
			require.NotNil(t, conn)
			require.NotNil(t, sess)
			added <- struct{}{}
		})

		mux := newTestMux(t, ctx, 1, cf, sf, mgr)
		mux.Start()

		select {
		case <-added:
		case <-time.After(500 * time.Millisecond):
			t.Fatal("expected AddConnection to be called")
		}

		cancel()
		closeDone()
		mux.Wait()
	})

	t.Run("ping EOF causes retry then success", func(t *testing.T) {
		t.Parallel()

		ctx, cancel := context.WithCancel(t.Context())
		cf := NewMockConnectionFactory(ctrl)
		sf := NewMockSessionFactory(ctrl)
		mgr := NewMockConnectionManager(ctrl)

		doneCh, closeDone := makeDoneCh(t)
		added := make(chan struct{}, 1)

		// First: server yamux closed immediately → client ping gets EOF.
		failClientConn, failServerConn := net.Pipe()
		failSrv, err := yamux.Server(failServerConn, nil)
		require.NoError(t, err)
		_ = failSrv.Close()
		_ = failServerConn.Close()

		goodClientConn, _ := newYamuxServerPair(t)

		cf.EXPECT().Done().Return(doneCh).AnyTimes()
		gomock.InOrder(
			cf.EXPECT().NewConnection().Return(failClientConn, nil),
			cf.EXPECT().NewConnection().Return(goodClientConn, nil),
		)
		sf.EXPECT().NewSession(gomock.Any()).DoAndReturn(func(conn net.Conn) (*yamux.Session, error) {
			return yamuxClientSession(t, conn), nil
		}).AnyTimes()
		mgr.EXPECT().AddConnection(gomock.Any(), gomock.Any()).Do(func(net.Conn, *yamux.Session) {
			added <- struct{}{}
		})

		mux := newTestMux(t, ctx, 1, cf, sf, mgr)
		mux.Start()

		select {
		case <-added:
		case <-time.After(500 * time.Millisecond):
			t.Fatal("expected AddConnection after EOF retry")
		}

		cancel()
		closeDone()
		mux.Wait()
	})

	t.Run("ping write timeout causes retry then success", func(t *testing.T) {
		t.Parallel()

		ctx, cancel := context.WithCancel(t.Context())
		cf := NewMockConnectionFactory(ctrl)
		sf := NewMockSessionFactory(ctrl)
		mgr := NewMockConnectionManager(ctrl)

		doneCh, closeDone := makeDoneCh(t)
		added := make(chan struct{}, 1)

		// First: raw pipe where server never speaks yamux + short write timeout.
		timeoutClientConn, timeoutServerConn := net.Pipe()
		t.Cleanup(func() {
			_ = timeoutClientConn.Close()
			_ = timeoutServerConn.Close()
		})
		shortCfg := yamux.DefaultConfig()
		shortCfg.ConnectionWriteTimeout = 10 * time.Millisecond

		goodClientConn, _ := newYamuxServerPair(t)

		cf.EXPECT().Done().Return(doneCh).AnyTimes()
		gomock.InOrder(
			cf.EXPECT().NewConnection().Return(timeoutClientConn, nil),
			cf.EXPECT().NewConnection().Return(goodClientConn, nil),
		)
		gomock.InOrder(
			sf.EXPECT().NewSession(gomock.Any()).DoAndReturn(func(conn net.Conn) (*yamux.Session, error) {
				return yamux.Client(conn, shortCfg)
			}),
			sf.EXPECT().NewSession(gomock.Any()).DoAndReturn(func(conn net.Conn) (*yamux.Session, error) {
				return yamuxClientSession(t, conn), nil
			}),
		)
		mgr.EXPECT().AddConnection(gomock.Any(), gomock.Any()).Do(func(net.Conn, *yamux.Session) {
			added <- struct{}{}
		})

		mux := newTestMux(t, ctx, 1, cf, sf, mgr)
		mux.Start()

		select {
		case <-added:
		case <-time.After(2 * time.Second):
			t.Fatal("expected AddConnection after write-timeout retry")
		}

		cancel()
		closeDone()
		mux.Wait()
	})

	t.Run("ping unknown error causes retry then success", func(t *testing.T) {
		t.Parallel()

		ctx, cancel := context.WithCancel(t.Context())
		cf := NewMockConnectionFactory(ctrl)
		sf := NewMockSessionFactory(ctrl)
		mgr := NewMockConnectionManager(ctrl)

		doneCh, closeDone := makeDoneCh(t)
		added := make(chan struct{}, 1)

		// First: close raw serverConn (not yamux) → broken pipe on ping.
		failClientConn, failServerConn := net.Pipe()
		_ = failServerConn.Close()

		goodClientConn, _ := newYamuxServerPair(t)

		cf.EXPECT().Done().Return(doneCh).AnyTimes()
		gomock.InOrder(
			cf.EXPECT().NewConnection().Return(failClientConn, nil),
			cf.EXPECT().NewConnection().Return(goodClientConn, nil),
		)
		sf.EXPECT().NewSession(gomock.Any()).DoAndReturn(func(conn net.Conn) (*yamux.Session, error) {
			return yamuxClientSession(t, conn), nil
		}).AnyTimes()
		mgr.EXPECT().AddConnection(gomock.Any(), gomock.Any()).Do(func(net.Conn, *yamux.Session) {
			added <- struct{}{}
		})

		mux := newTestMux(t, ctx, 1, cf, sf, mgr)
		mux.Start()

		select {
		case <-added:
		case <-time.After(500 * time.Millisecond):
			t.Fatal("expected AddConnection after unknown-error retry")
		}

		cancel()
		closeDone()
		mux.Wait()
	})

	t.Run("context cancel during ping failure stops loop", func(t *testing.T) {
		t.Parallel()

		ctx, cancel := context.WithCancel(t.Context())
		cf := NewMockConnectionFactory(ctrl)
		sf := NewMockSessionFactory(ctrl)
		mgr := NewMockConnectionManager(ctrl)

		doneCh, closeDone := makeDoneCh(t)
		cf.EXPECT().Done().Return(doneCh).AnyTimes()
		cf.EXPECT().NewConnection().DoAndReturn(func() (net.Conn, error) {
			clientConn, serverConn := net.Pipe()
			srv, err := yamux.Server(serverConn, nil)
			if err != nil {
				_ = serverConn.Close()
				_ = clientConn.Close()
				return nil, err
			}
			_ = srv.Close()
			_ = serverConn.Close()
			return clientConn, nil
		}).AnyTimes()
		sf.EXPECT().NewSession(gomock.Any()).DoAndReturn(func(conn net.Conn) (*yamux.Session, error) {
			return yamuxClientSession(t, conn), nil
		}).AnyTimes()

		mux := newTestMux(t, ctx, 1, cf, sf, mgr)
		mux.Start()

		// Allow the retry loop to execute a few iterations before canceling, so we
		// exercise the context-cancel-during-ping path rather than the never-started path.
		time.Sleep(30 * time.Millisecond)
		cancel()
		closeDone()

		done := make(chan struct{})
		go func() { mux.Wait(); close(done) }()

		select {
		case <-done:
		case <-time.After(500 * time.Millisecond):
			t.Fatal("Wait() did not return after context cancel")
		}
	})
}

func TestMux_MultipleConnections(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)

	t.Run("creates exactly numConns connections", func(t *testing.T) {
		t.Parallel()

		ctx, cancel := context.WithCancel(t.Context())
		cf := NewMockConnectionFactory(ctrl)
		sf := NewMockSessionFactory(ctrl)
		mgr := NewMockConnectionManager(ctrl)

		doneCh, closeDone := makeDoneCh(t)
		var count atomic.Int32
		allAdded := make(chan struct{})

		cf.EXPECT().Done().Return(doneCh).AnyTimes()
		cf.EXPECT().NewConnection().DoAndReturn(func() (net.Conn, error) {
			clientConn, serverConn := net.Pipe()
			go func() {
				srv, err := yamux.Server(serverConn, nil)
				if err != nil {
					return
				}
				<-srv.CloseChan()
				_ = srv.Close()
				_ = serverConn.Close()
			}()
			return clientConn, nil
		}).Times(3)
		sf.EXPECT().NewSession(gomock.Any()).DoAndReturn(func(conn net.Conn) (*yamux.Session, error) {
			return yamuxClientSession(t, conn), nil
		}).Times(3)
		mgr.EXPECT().AddConnection(gomock.Any(), gomock.Any()).Do(func(net.Conn, *yamux.Session) {
			if count.Add(1) == 3 {
				close(allAdded)
			}
		}).Times(3)

		mux := newTestMux(t, ctx, 3, cf, sf, mgr)
		mux.Start()

		select {
		case <-allAdded:
		case <-time.After(time.Second):
			t.Fatal("expected 3 connections")
		}

		// Brief pause to give the goroutine time to attempt a fourth connection, which
		// would violate the mock's Times(3) expectation and catch semaphore leaks.
		time.Sleep(20 * time.Millisecond)
		require.Equal(t, int32(3), count.Load())

		cancel()
		closeDone()
		mux.Wait()
	})
}

func TestMux_GracefulShutdown(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)

	t.Run("context cancel unblocks semaphore acquire and exits", func(t *testing.T) {
		t.Parallel()

		ctx, cancel := context.WithCancel(t.Context())
		cf := NewMockConnectionFactory(ctrl)
		sf := NewMockSessionFactory(ctrl)
		mgr := NewMockConnectionManager(ctrl)

		doneCh, closeDone := makeDoneCh(t)
		cf.EXPECT().Done().Return(doneCh).AnyTimes()
		cf.EXPECT().NewConnection().DoAndReturn(func() (net.Conn, error) {
			<-ctx.Done()
			return nil, ctx.Err()
		}).AnyTimes()

		mux := newTestMux(t, ctx, 1, cf, sf, mgr)
		mux.Start()

		cancel()
		closeDone()

		done := make(chan struct{})
		go func() { mux.Wait(); close(done) }()

		select {
		case <-done:
		case <-time.After(500 * time.Millisecond):
			t.Fatal("Wait() did not return after context cancel")
		}
	})

	t.Run("Done closes only after factory Done closes", func(t *testing.T) {
		t.Parallel()

		ctx, cancel := context.WithCancel(t.Context())
		cf := NewMockConnectionFactory(ctrl)
		sf := NewMockSessionFactory(ctrl)
		mgr := NewMockConnectionManager(ctrl)

		doneCh, closeDone := makeDoneCh(t)
		cf.EXPECT().Done().Return(doneCh).AnyTimes()
		cf.EXPECT().NewConnection().DoAndReturn(func() (net.Conn, error) {
			<-ctx.Done()
			return nil, ctx.Err()
		}).AnyTimes()

		mux := newTestMux(t, ctx, 1, cf, sf, mgr)
		mux.Start()

		cancel()

		// Done() must NOT be closed yet — mux waits for connFn.Done() first.
		select {
		case <-mux.Done():
			t.Fatal("Done() should not be closed before factory Done() closes")
		case <-time.After(30 * time.Millisecond):
		}

		closeDone()

		require.Eventually(t, func() bool {
			select {
			case <-mux.Done():
				return true
			default:
				return false
			}
		}, 200*time.Millisecond, 5*time.Millisecond)
	})
}

func newTestMux(
	t *testing.T,
	ctx context.Context,
	numConns int,
	cf transport.ConnectionFactory,
	sf transport.SessionFactory,
	cm transport.ConnectionManager,
) *transport.Mux {
	t.Helper()

	mux, err := transport.NewMux(ctx, "test-mux", numConns,
		transport.WithConnectionFactory(cf),
		transport.WithSessionFactory(sf),
		transport.WithConnectionManager(cm),
	)

	require.NoError(t, err)
	return mux
}

// makeDoneCh returns a receive-only done channel and an idempotent close func.
// The close func is also registered as t.Cleanup so it fires on test exit.
func makeDoneCh(t *testing.T) (<-chan struct{}, func()) {
	t.Helper()

	ch := make(chan struct{})
	closeOnce := sync.OnceFunc(func() { close(ch) })
	t.Cleanup(closeOnce)

	return ch, closeOnce
}

// newYamuxServerPair creates a net.Pipe with yamux.Server on the server side.
// Returns the raw client conn (pass to yamux.Client in the session factory)
// and the server session (must stay alive for ping to succeed).
func newYamuxServerPair(t *testing.T) (net.Conn, *yamux.Session) {
	t.Helper()

	clientConn, serverConn := net.Pipe()
	serverSession, err := yamux.Server(serverConn, nil)
	require.NoError(t, err)

	t.Cleanup(func() {
		_ = serverSession.Close()
		_ = serverConn.Close()
		_ = clientConn.Close()
	})

	return clientConn, serverSession
}

func yamuxClientSession(t *testing.T, conn net.Conn) *yamux.Session {
	t.Helper()

	sess, err := yamux.Client(conn, nil)
	require.NoError(t, err)
	return sess
}

func (c *closeTrackingConn) Close() error {
	c.closed.Add(1)
	return c.Conn.Close()
}
