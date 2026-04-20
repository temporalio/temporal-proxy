package server

import (
	"context"
	"net"
	"time"

	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"

	"github.com/temporalio/temporal-proxy/internal/transport/creds"
)

type (
	Server struct {
		grpcSvr   *grpc.Server
		healthSvr *health.Server

		cancelFunc  context.CancelFunc
		healthCheck HealthCheck
		logger      log.Logger
	}

	Credentials interface {
		ServerOption() (grpc.ServerOption, error)
	}
)

func New(sopts ...Option) (*Server, error) {
	opts := &options{
		creds:       creds.NewInsecure(),
		healthCheck: defaultHealthCheck(),
		logger:      log.NewNoopLogger(),
	}
	for _, opt := range sopts {
		opt.apply(opts)
	}

	svrOpts, err := opts.serverOptions()
	if err != nil {
		return nil, err
	}

	svr := grpc.NewServer(svrOpts...)

	// add health check
	hc := health.NewServer()
	grpc_health_v1.RegisterHealthServer(svr, hc)

	if opts.reflect {
		reflection.Register(svr)
	}

	return &Server{
		grpcSvr:     svr,
		healthSvr:   hc,
		healthCheck: opts.healthCheck,
		logger:      opts.logger,
	}, nil
}

func (s *Server) Start(ctx context.Context, lis net.Listener) error {
	s.logger = log.With(s.logger, tag.NewStringerTag("addr", lis.Addr()))
	s.logger.Info("Starting the server")

	ctx, s.cancelFunc = context.WithCancel(ctx)
	go s.runHealthCheck(ctx)
	return s.grpcSvr.Serve(lis)
}

func (s *Server) Stop(ctx context.Context) error {
	s.logger.Info("Shutting down")
	if s.cancelFunc != nil {
		s.cancelFunc()
	}

	s.grpcSvr.GracefulStop()
	return nil
}

func (s *Server) runHealthCheck(ctx context.Context) {
	next := grpc_health_v1.HealthCheckResponse_SERVING

	for {
		s.healthSvr.SetServingStatus("", next)

		select {
		case <-ctx.Done():
			s.healthSvr.Shutdown()
			return
		case <-time.After(s.healthCheck.Interval()):
			next = s.healthCheck.Status(ctx)
		}
	}
}
