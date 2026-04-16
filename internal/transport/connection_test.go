package transport_test

import (
	"context"
	"crypto/tls"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/backoff"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.uber.org/mock/gomock"

	"github.com/temporalio/temporal-proxy/internal/transport"
)

var fastRetryPolicy = backoff.NewExponentialRetryPolicy(5 * time.Millisecond).
	WithMaximumInterval(10 * time.Millisecond)

func TestNewInboundFactory(t *testing.T) {
	t.Parallel()

	t.Run("nil opts returns valid factory", func(t *testing.T) {
		t.Parallel()

		f, err := transport.NewInboundFactory(t.Context(), "127.0.0.1:0", nil)
		require.NoError(t, err)
		require.NotNil(t, f)
		require.NotEmpty(t, f.Address())

		// NB: Done channel is NOT pre-closed for InboundFactory — it closes only on shutdown.
		select {
		case <-f.Done():
			t.Fatal("Done() channel should not be closed yet")
		default:
		}
	})

	t.Run("returns error for invalid address", func(t *testing.T) {
		t.Parallel()

		_, err := transport.NewInboundFactory(t.Context(), "invalid-addr", nil)
		require.Error(t, err)
	})

	t.Run("returns error when port is already in use", func(t *testing.T) {
		t.Parallel()

		ln, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		t.Cleanup(func() { _ = ln.Close() })

		_, err = transport.NewInboundFactory(t.Context(), ln.Addr().String(), nil)
		require.Error(t, err)
	})
}

func TestInboundFactory_NewConnection(t *testing.T) {
	t.Parallel()

	t.Run("returns accepted connection", func(t *testing.T) {
		t.Parallel()

		f, err := transport.NewInboundFactory(t.Context(), "127.0.0.1:0", nil)
		require.NoError(t, err)

		go func() {
			c, err := net.Dial("tcp", f.Address())
			if err == nil {
				_ = c.Close()
			}
		}()

		conn, err := f.NewConnection()
		require.NoError(t, err)
		require.NotNil(t, conn)
		require.NoError(t, conn.Close())
	})

	t.Run("context canceled returns context error", func(t *testing.T) {
		t.Parallel()

		ctx, cancel := context.WithCancel(t.Context())
		cancel() // pre-cancel

		f, err := transport.NewInboundFactory(ctx, "127.0.0.1:0", nil)
		require.NoError(t, err)

		// Dial to unblock Accept so NewConnection can check ctx.Err().
		go func() {
			c, err := net.Dial("tcp", f.Address())
			if err == nil {
				_ = c.Close()
			}
		}()

		_, err = f.NewConnection()
		require.ErrorIs(t, err, context.Canceled)
	})

	t.Run("TLS adapter wraps connection", func(t *testing.T) {
		t.Parallel()

		f, err := transport.NewInboundFactory(t.Context(), "127.0.0.1:0", &transport.FactoryOptions{
			TLS: &tls.Config{InsecureSkipVerify: true}, //nolint:gosec // test only
		})
		require.NoError(t, err)

		go func() {
			c, err := net.Dial("tcp", f.Address())
			if err == nil {
				_ = c.Close()
			}
		}()

		conn, err := f.NewConnection()
		require.NoError(t, err)
		require.NotNil(t, conn)

		_, ok := conn.(*tls.Conn)
		require.True(t, ok, "expected connection to be *tls.Conn")
		require.NoError(t, conn.Close())
	})
}

func TestNewOutboundFactory(t *testing.T) {
	t.Parallel()

	t.Run("nil opts returns valid factory", func(t *testing.T) {
		t.Parallel()

		f := transport.NewOutboundFactory(t.Context(), "127.0.0.1:1234", nil)
		require.NotNil(t, f)
		require.Equal(t, "127.0.0.1:1234", f.Address())

		select {
		case <-f.Done(): // Nb: channel is always closed for OutputFactory.
		default:
			t.Fatal("Done() channel should already be closed")
		}
	})

	t.Run("with opts.Log does not panic", func(t *testing.T) {
		t.Parallel()

		f := transport.NewOutboundFactory(t.Context(), "127.0.0.1:1234", &transport.FactoryOptions{})
		require.NotNil(t, f)
	})

	t.Run("applies supplied options", func(t *testing.T) {
		t.Parallel()

		ctx, cancel := context.WithCancel(t.Context())
		cancel() // pre-cancel so NewConnection returns immediately

		ctrl := gomock.NewController(t)
		logger := log.NewMockLogger(ctrl)
		logger.EXPECT().Debug("Attempting to dial", tag.String("addr", "127.0.0.1:1"))

		f := transport.NewOutboundFactory(ctx, "127.0.0.1:1", &transport.FactoryOptions{
			Log:         logger,
			RetryPolicy: fastRetryPolicy,
		})

		start := time.Now()
		_, err := f.NewConnection()
		elapsed := time.Since(start)
		require.Error(t, err)

		// With a fast policy the context cancellation exits quickly; the default 1s policy
		// would make this significantly slower if not respected.
		require.Less(t, elapsed, 500*time.Millisecond, "expected fast exit with custom retry policy")
	})
}

func TestOutboundFactory_NewConnection(t *testing.T) {
	t.Parallel()

	t.Run("success on first attempt", func(t *testing.T) {
		t.Parallel()

		ln, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		t.Cleanup(func() { _ = ln.Close() })

		go func() {
			c, _ := ln.Accept()
			if c != nil {
				_ = c.Close()
			}
		}()

		f := transport.NewOutboundFactory(t.Context(), ln.Addr().String(), &transport.FactoryOptions{
			RetryPolicy: fastRetryPolicy,
		})

		conn, err := f.NewConnection()
		require.NoError(t, err)
		require.NotNil(t, conn)
		require.NoError(t, conn.Close())
	})

	t.Run("retries until listener becomes available", func(t *testing.T) {
		t.Parallel()

		// Reserve a port then release it so the factory dials a closed port initially.
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)

		addr := ln.Addr().String()
		require.NoError(t, ln.Close())

		// Re-open the listener after a short delay so the factory retries a couple of times.
		go func() {
			time.Sleep(30 * time.Millisecond)
			ln2, err := net.Listen("tcp", addr)
			if err != nil {
				return
			}
			defer func() { _ = ln2.Close() }()

			c, _ := ln2.Accept()
			if c != nil {
				_ = c.Close()
			}
		}()

		f := transport.NewOutboundFactory(t.Context(), addr, &transport.FactoryOptions{
			RetryPolicy: fastRetryPolicy,
		})

		conn, err := f.NewConnection()
		require.NoError(t, err)
		require.NotNil(t, conn)
		require.NoError(t, conn.Close())
	})

	t.Run("context canceled returns context error", func(t *testing.T) {
		t.Parallel()

		ctx, cancel := context.WithCancel(t.Context())
		cancel() // pre-cancel

		f := transport.NewOutboundFactory(ctx, "127.0.0.1:1", &transport.FactoryOptions{
			RetryPolicy: fastRetryPolicy,
		})

		_, err := f.NewConnection()
		require.ErrorIs(t, err, context.Canceled)
	})

	t.Run("context canceled during retry exits loop", func(t *testing.T) {
		t.Parallel()

		ctx, cancel := context.WithCancel(t.Context())

		// Nothing is listening on port 1; factory will keep failing.
		f := transport.NewOutboundFactory(ctx, "127.0.0.1:1", &transport.FactoryOptions{
			RetryPolicy: fastRetryPolicy,
		})

		go func() {
			time.Sleep(30 * time.Millisecond)
			cancel()
		}()

		_, err := f.NewConnection()
		require.Error(t, err)
	})

	t.Run("TLS adapter wraps connection", func(t *testing.T) {
		t.Parallel()

		ln, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		t.Cleanup(func() { _ = ln.Close() })

		go func() {
			c, _ := ln.Accept()
			if c != nil {
				_ = c.Close()
			}
		}()

		f := transport.NewOutboundFactory(t.Context(), ln.Addr().String(), &transport.FactoryOptions{
			TLS:         &tls.Config{InsecureSkipVerify: true}, //nolint:gosec // test only
			RetryPolicy: fastRetryPolicy,
		})

		conn, err := f.NewConnection()
		require.NoError(t, err)
		require.NotNil(t, conn)
		_, ok := conn.(*tls.Conn)
		require.True(t, ok, "expected connection to be wrapped in *tls.Conn")
		_ = conn.Close()
	})
}
