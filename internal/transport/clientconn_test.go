package transport_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"

	"github.com/temporalio/temporal-proxy/internal/transport"
)

func TestNewClientConn(t *testing.T) {
	t.Parallel()

	t.Run("creates successfully", func(t *testing.T) {
		t.Parallel()

		cc := newClientConn(t, t.Context(), "test")
		require.NotNil(t, cc)
	})

	t.Run("context cancellation closes the conn", func(t *testing.T) {
		t.Parallel()

		ctx, cancel := context.WithCancel(t.Context())
		cc := newClientConn(t, ctx, "test")

		cancel()
		require.Eventually(t, func() bool { return !cc.IsReady() }, 500*time.Millisecond, 5*time.Millisecond)
	})
}

func TestClientConn_Close(t *testing.T) {
	t.Parallel()

	cc := newClientConn(t, t.Context(), "test")
	require.NoError(t, cc.Close())
}

func TestClientConn_IsReady(t *testing.T) {
	t.Parallel()

	t.Run("returns false with no sessions", func(t *testing.T) {
		t.Parallel()

		cc := newClientConn(t, t.Context(), "test")
		require.False(t, cc.IsReady())
	})

	t.Run("returns true after sessions are added", func(t *testing.T) {
		t.Parallel()

		pipe := newYamuxPipe(t)
		cc := newClientConn(t, t.Context(), "test")

		s := transport.NewSession(t.Context(), "s0", pipe.conn, pipe.session)
		t.Cleanup(func() { _ = s.Close() })

		cc.OnSessionsUpdated(map[string]*transport.Session{"session-0:0": s})
		require.True(t, cc.IsReady())
	})

	t.Run("returns false after context is canceled", func(t *testing.T) {
		t.Parallel()

		pipe := newYamuxPipe(t)
		ctx, cancel := context.WithCancel(t.Context())
		cc := newClientConn(t, ctx, "test")

		s := transport.NewSession(ctx, "s0", pipe.conn, pipe.session)
		t.Cleanup(func() { _ = s.Close() })

		cc.OnSessionsUpdated(map[string]*transport.Session{"session-0:0": s})
		require.True(t, cc.IsReady())

		cancel()
		require.Eventually(t, func() bool { return !cc.IsReady() }, 500*time.Millisecond, 5*time.Millisecond)
	})
}

func TestClientConn_OnSessionsUpdated(t *testing.T) {
	t.Parallel()

	t.Run("empty map makes conn not ready", func(t *testing.T) {
		t.Parallel()

		pipe := newYamuxPipe(t)
		cc := newClientConn(t, t.Context(), "test")

		s := transport.NewSession(t.Context(), "s0", pipe.conn, pipe.session)
		t.Cleanup(func() { _ = s.Close() })

		cc.OnSessionsUpdated(map[string]*transport.Session{"session-0:0": s})
		require.True(t, cc.IsReady())

		cc.OnSessionsUpdated(nil)
		require.False(t, cc.IsReady())
	})

	t.Run("sessions map makes conn ready", func(t *testing.T) {
		t.Parallel()

		pipe := newYamuxPipe(t)
		cc := newClientConn(t, t.Context(), "test")

		s := transport.NewSession(t.Context(), "s0", pipe.conn, pipe.session)
		t.Cleanup(func() { _ = s.Close() })

		cc.OnSessionsUpdated(map[string]*transport.Session{"session-0:0": s})
		require.True(t, cc.IsReady())
	})

	t.Run("multiple sessions are registered", func(t *testing.T) {
		t.Parallel()

		pipe1 := newYamuxPipe(t)
		pipe2 := newYamuxPipe(t)
		cc := newClientConn(t, t.Context(), "test")

		s1 := transport.NewSession(t.Context(), "s1", pipe1.conn, pipe1.session)
		s2 := transport.NewSession(t.Context(), "s2", pipe2.conn, pipe2.session)
		t.Cleanup(func() { _ = s1.Close() })
		t.Cleanup(func() { _ = s2.Close() })

		cc.OnSessionsUpdated(map[string]*transport.Session{
			"session-0:0": s1,
			"session-1:0": s2,
		})
		require.True(t, cc.IsReady())
	})
}

func TestClientConn_String(t *testing.T) {
	t.Parallel()

	t.Run("contains name and scheme", func(t *testing.T) {
		t.Parallel()

		cc := newClientConn(t, t.Context(), "my-conn")
		str := cc.String()
		require.Contains(t, str, "name:my-conn")
		require.Contains(t, str, "scheme:tmprl")
	})

	t.Run("contains session address after update", func(t *testing.T) {
		t.Parallel()

		pipe := newYamuxPipe(t)
		cc := newClientConn(t, t.Context(), "test")

		s := transport.NewSession(t.Context(), "s0", pipe.conn, pipe.session)
		t.Cleanup(func() { _ = s.Close() })

		cc.OnSessionsUpdated(map[string]*transport.Session{"session-0:0": s})
		require.Contains(t, cc.String(), "session-0:0")
	})
}

func TestClientConn_EndToEnd(t *testing.T) {
	t.Parallel()

	t.Run("NewStream routes through yamux session", func(t *testing.T) {
		t.Parallel()

		pipe := newYamuxPipe(t)

		grpcSvr := grpc.NewServer()
		grpc_health_v1.RegisterHealthServer(grpcSvr, health.NewServer())
		go func() { _ = grpcSvr.Serve(pipe.serverSession) }()
		t.Cleanup(grpcSvr.Stop)

		session := transport.NewSession(t.Context(), "s0", pipe.conn, pipe.session)
		t.Cleanup(func() { _ = session.Close() })

		cc := newClientConn(t, t.Context(), "test")
		cc.OnSessionsUpdated(map[string]*transport.Session{"session-0:0": session})

		require.Eventually(t, cc.IsReady, 500*time.Millisecond, 5*time.Millisecond)

		// Watch is a server-streaming RPC, so it exercises ClientConn.NewStream.
		healthClient := grpc_health_v1.NewHealthClient(cc)
		stream, err := healthClient.Watch(t.Context(), &grpc_health_v1.HealthCheckRequest{})
		require.NoError(t, err)

		_, err = stream.Recv()
		require.NoError(t, err)
	})

	t.Run("Invoke routes through yamux session", func(t *testing.T) {
		t.Parallel()

		pipe := newYamuxPipe(t)

		// Serve gRPC health over the yamux server session (yamux.Session satisfies net.Listener).
		grpcSvr := grpc.NewServer()
		grpc_health_v1.RegisterHealthServer(grpcSvr, health.NewServer())
		go func() { _ = grpcSvr.Serve(pipe.serverSession) }()
		t.Cleanup(grpcSvr.Stop)

		// Wrap the client yamux session in a transport.Session.
		session := transport.NewSession(t.Context(), "s0", pipe.conn, pipe.session)
		t.Cleanup(func() { _ = session.Close() })

		// Build the ClientConn and register the session.
		cc := newClientConn(t, t.Context(), "test")
		cc.OnSessionsUpdated(map[string]*transport.Session{"session-0:0": session})

		require.Eventually(t, cc.IsReady, 500*time.Millisecond, 5*time.Millisecond)

		// Make a real gRPC call through the ClientConn.
		healthClient := grpc_health_v1.NewHealthClient(cc)
		resp, err := healthClient.Check(t.Context(), &grpc_health_v1.HealthCheckRequest{})
		require.NoError(t, err)
		require.Equal(t, grpc_health_v1.HealthCheckResponse_SERVING, resp.GetStatus())
	})
}

func newClientConn(t *testing.T, ctx context.Context, name string, opts ...grpc.DialOption) *transport.ClientConn {
	t.Helper()

	opts = append([]grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}, opts...)
	cc, err := transport.NewClientConn(ctx, name, opts...)
	require.NoError(t, err)
	t.Cleanup(func() { _ = cc.Close() })
	return cc
}
