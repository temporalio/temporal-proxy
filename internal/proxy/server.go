package proxy

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"

	"go.temporal.io/api/proxy"
	"go.temporal.io/api/workflowservice/v1"
	"google.golang.org/grpc"

	"github.com/temporalio/temporal-proxy/internal/server"
	"github.com/temporalio/temporal-proxy/internal/transport/creds"
	"github.com/temporalio/temporal-proxy/internal/transport/socket"
	"github.com/temporalio/temporal-proxy/pkg/logger"
)

type (
	// Credentials produces the [grpc.DialOption] used to secure the outbound
	// connection to the upstream Temporal frontend.
	Credentials interface {
		DialOption() (grpc.DialOption, error)
	}

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
		logger logger.Logger
	}

	// Option configures a [Server] via [New].
	Option func(*Options)
)

// New constructs a Server that forwards WorkflowService traffic to the
// upstream reachable through cc. The local listener is a unix socket whose
// path is derived from hostPort. cc is typically a resolvingConn; the
// connection(s) it uses are owned by the shared pool, not by this Server.
func New(hostPort string, cc grpc.ClientConnInterface, opts ...Option) (*Server, error) {
	pops := &Options{logger: logger.Default()}
	for _, opt := range opts {
		opt(pops)
	}

	wfs, err := proxy.NewWorkflowServiceProxyServer(workflowProxyOptions(cc))
	if err != nil {
		return nil, fmt.Errorf("failed to create workflowservice proxy: %w", err)
	}

	svr, err := server.New(
		// NB: Hosting on local unix port, no need for TLS here.
		server.WithCredentials(creds.NewInsecure()),
		server.WithLogger(pops.logger),
		server.WithService(func(sr grpc.ServiceRegistrar) {
			workflowservice.RegisterWorkflowServiceServer(sr, wfs)
		}),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create proxy: %s, %w", hostPort, err)
	}

	path, err := socket.UnixPath(hostPort)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve socket path: %w", err)
	}

	return &Server{svr: svr, path: path}, nil
}

// WithLogger sets the logger used by the proxy.
func WithLogger(log logger.Logger) Option {
	return Option(func(o *Options) { o.logger = log })
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

// Start serves on lis until the proxy is stopped or ctx is cancelled. It
// blocks, so callers typically run it in its own goroutine after binding the
// listener with Listen.
func (s *Server) Start(ctx context.Context, lis net.Listener) error {
	return s.svr.Start(ctx, lis)
}

// Stop gracefully shuts the proxy down, waiting for in-flight RPCs to complete.
func (s *Server) Stop(ctx context.Context) error {
	if err := s.svr.Stop(ctx); err != nil {
		return fmt.Errorf("failed to stop GRPC server: %w", err)
	}

	return nil
}

// workflowProxyOptions builds the options for the WorkflowService proxy
// server backed by cc. DisableHeaderForwarding is intentionally left false:
// the upstream proxy must forward incoming metadata (including the
// router-stamped namespace) onto the outbound call, since templated upstream
// resolution and namespace translation both depend on it.
func workflowProxyOptions(cc grpc.ClientConnInterface) proxy.WorkflowServiceProxyOptions {
	return proxy.WorkflowServiceProxyOptions{Client: workflowservice.NewWorkflowServiceClient(cc)}
}
