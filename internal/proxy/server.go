package proxy

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"

	"github.com/temporalio/temporal-proxy/internal/server"
	"github.com/temporalio/temporal-proxy/internal/transport/creds"
	"github.com/temporalio/temporal-proxy/internal/transport/socket"
	"github.com/temporalio/temporal-proxy/pkg/logger"
)

type (
	// Server proxies the Temporal WorkflowService. It re-serves an upstream
	// frontend on a local unix socket, letting local workers connect without TLS
	// while the upstream hop stays secured. The upstream connection(s) it
	// forwards to are owned by the shared [connect.Pool], not by this Server.
	Server struct {
		svr  *server.Server
		path string // path to unix socket
	}

	// Options configures a [Server] at construction time.
	Options struct {
		logger     logger.Logger
		socketPath string
	}

	// Option configures a [Server] via [New].
	Option func(*Options)
)

// New constructs a Server that hands every inbound method to fw, which forwards
// it to the upstream fw was built against. The local listener is a unix socket
// whose path is derived from hostPort. The connection(s) fw forwards over are
// owned by the shared pool, not by this Server.
func New(hostPort string, fw *Forwarder, opts ...Option) (*Server, error) {
	pops := &Options{logger: logger.Default()}
	for _, opt := range opts {
		opt(pops)
	}

	svr, err := server.New(
		// NB: Hosting on local unix port, no need for TLS here.
		server.WithCredentials(creds.NewListener(creds.Insecure())),
		server.WithLogger(pops.logger),
		server.WithUnknownServiceHandler(fw.Handle),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create proxy: %s, %w", hostPort, err)
	}

	path := pops.socketPath
	if path == "" {
		p, err := socket.UnixPath(hostPort)
		if err != nil {
			return nil, fmt.Errorf("failed to resolve socket path: %w", err)
		}

		path = p
	} else if err := socket.ValidatePath(path); err != nil {
		return nil, fmt.Errorf("invalid socket path: %w", err)
	}

	return &Server{svr: svr, path: path}, nil
}

// WithLogger sets the logger used by the proxy.
func WithLogger(log logger.Logger) Option {
	return Option(func(o *Options) { o.logger = log })
}

// WithSocketPath sets the unix socket path the proxy binds, overriding the one
// derived from hostPort. A caller that also dials this socket passes the same
// value to both sides so the two cannot disagree. [New] rejects a path that
// exceeds the platform's sun_path limit.
func WithSocketPath(path string) Option {
	return Option(func(o *Options) { o.socketPath = path })
}

// Listen removes any socket left behind by a prior run and binds the proxy's
// local unix socket, returning the listener. Binding is separate from Start so
// callers can bind synchronously during startup (the socket is then listening,
// and the OS backlogs connections) before serving in the background, ensuring
// no request is routed to an unbound socket.
func (s *Server) Listen(ctx context.Context) (net.Listener, error) {
	// Remove any socket left behind by a prior run; otherwise the bind fails
	// with "address already in use".
	if err := os.Remove(s.path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf("failed to remove stale socket: unix://%s, %w", s.path, err)
	}

	lis, err := (&net.ListenConfig{}).Listen(ctx, "unix", s.path)
	if err != nil {
		return nil, fmt.Errorf("failed to bind to socket: unix://%s, %w", s.path, err)
	}

	return lis, nil
}

// Start serves on lis until Stop is called; ctx is not what stops it, and is
// used only to drive the periodic health check. It blocks, so callers
// typically run it in its own goroutine after binding the listener with
// Listen.
func (s *Server) Start(ctx context.Context, lis net.Listener) error {
	return s.svr.Start(ctx, lis)
}

// Stop shuts the proxy down, draining in-flight RPCs within the server's
// shutdown budget and dropping whatever is left.
func (s *Server) Stop(ctx context.Context) error {
	if err := s.svr.Stop(ctx); err != nil {
		return fmt.Errorf("failed to stop GRPC server: %w", err)
	}

	return nil
}
