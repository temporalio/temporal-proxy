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

	// Server proxies the Temporal WorkflowService. It dials an upstream frontend
	// and re-serves it on a local unix socket, letting local workers connect
	// without TLS while the upstream hop stays secured.
	Server struct {
		svr  *server.Server
		conn *grpc.ClientConn

		host string // upstream hostPort
		path string // path to unix socket
	}

	// Options configures a [Server] at construction time.
	Options struct {
		creds  Credentials
		logger logger.Logger
	}

	// Option configures a [Server] via [New].
	Option func(*Options)
)

// New constructs a [Server] that forwards WorkflowService traffic to the
// upstream frontend at hostPort. The local listener is a unix socket whose path
// is derived from hostPort (see [Server.Start]). With no options it dials the
// upstream with insecure credentials and logs via a CLI logger.
func New(hostPort string, opts ...Option) (*Server, error) {
	pops := &Options{
		creds:  creds.NewInsecure(),
		logger: logger.Default(),
	}
	for _, opt := range opts {
		opt(pops)
	}

	upstreamCreds, err := pops.creds.DialOption()
	if err != nil {
		return nil, fmt.Errorf("failed to generate outbound credentials: %w", err)
	}

	conn, err := grpc.NewClient(hostPort, upstreamCreds)
	if err != nil {
		return nil, fmt.Errorf("failed to dial: %s, %w", hostPort, err)
	}

	wfs, err := proxy.NewWorkflowServiceProxyServer(proxy.WorkflowServiceProxyOptions{
		Client: workflowservice.NewWorkflowServiceClient(conn),
	})
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

	return &Server{
		svr:  svr,
		conn: conn,
		host: hostPort,
		path: path,
	}, nil
}

// WithCredentials sets the transport credentials used to dial the upstream
// frontend.
func WithCredentials(creds Credentials) Option {
	return Option(func(o *Options) { o.creds = creds })
}

// WithLogger sets the logger used by the proxy.
func WithLogger(log logger.Logger) Option {
	return Option(func(o *Options) { o.logger = log })
}

// Start binds the local unix socket and serves until the proxy is stopped or
// ctx is cancelled. It first removes any socket left behind by a prior run. It
// blocks, so callers typically run it in its own goroutine.
func (s *Server) Start(ctx context.Context) error {
	// Remove any socket left behind by a prior run; otherwise the bind fails
	// with "address already in use".
	if err := os.Remove(s.path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("failed to remove stale socket: unix://%s, %w", s.path, err)
	}

	lis, err := (&net.ListenConfig{}).Listen(ctx, "unix", s.path)
	if err != nil {
		return fmt.Errorf("failed to bind to socket: unix://%s, %w", s.path, err)
	}

	return s.svr.Start(ctx, lis)
}

// Stop gracefully shuts the proxy down, waiting for in-flight RPCs to complete.
func (s *Server) Stop(ctx context.Context) error {
	if err := s.svr.Stop(ctx); err != nil {
		return fmt.Errorf("failed to stop GRPC server: %w", err)
	}

	return s.conn.Close()
}
