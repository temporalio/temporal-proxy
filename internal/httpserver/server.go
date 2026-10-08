package httpserver

import (
	"context"
	"crypto/tls"
	"errors"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/temporalio/temporal-proxy/pkg/logger"
	"github.com/temporalio/temporal-proxy/pkg/logger/tag"
)

const (
	// readHeaderTimeout and readTimeout bound a slow client. Unlike the gateway
	// no route served here long-polls, so a short budget is safe.
	readHeaderTimeout = 5 * time.Second
	readTimeout       = 10 * time.Second
)

// Server serves the HTTP route groups on one port, separate from the
// gateway's.
type Server struct {
	svr      *http.Server
	hostPort string
	abort    func(error)
	logger   logger.Logger

	// mu guards addr, which Start writes and a caller reads.
	mu   sync.Mutex
	addr net.Addr
}

// NewServer returns a Server that serves h on hostPort. It binds nothing,
// [Server.Start] does that.
func NewServer(
	hostPort string,
	h http.Handler,
	tlsCfg *tls.Config,
	log logger.Logger,
	abort func(error),
) *Server {
	return &Server{
		svr: &http.Server{
			Handler:           h,
			TLSConfig:         tlsCfg,
			ReadHeaderTimeout: readHeaderTimeout,
			ReadTimeout:       readTimeout,
		},
		hostPort: hostPort,
		abort:    abort,
		logger:   log,
	}
}

// Addr is the address the server is accepting on, including the port chosen
// for a port-zero hostPort, or nil before [Server.Start] returns successfully.
// Safe for concurrent use.
func (s *Server) Addr() net.Addr {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.addr
}

// Start binds the listener and serves in a background goroutine until
// [Server.Stop], returning once the listener is accepting. It returns an error
// only if the bind fails; a serving failure after that reaches the abort
// function given to [NewServer] instead.
//
// Call it at most once. A Server is not restartable after [Server.Stop].
func (s *Server) Start(ctx context.Context) error {
	lis, err := (&net.ListenConfig{}).Listen(ctx, "tcp", s.hostPort)
	if err != nil {
		return err
	}

	s.mu.Lock()
	s.addr = lis.Addr()
	s.mu.Unlock()

	log := s.logger.With(tag.Stringer("addr", lis.Addr()))
	log.Info("Starting the HTTP server")

	go func() {
		// ServeTLS with empty paths uses the certificates already on TLSConfig.
		serve := s.svr.Serve
		if s.svr.TLSConfig != nil {
			serve = func(l net.Listener) error { return s.svr.ServeTLS(l, "", "") }
		}

		if err := serve(lis); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("HTTP server stopped serving", tag.Error(err))
			if s.abort != nil {
				s.abort(err)
			}
		}
	}()

	return nil
}

// Stop closes the listener and waits for in-flight requests to finish, without
// calling the abort function. Returns ctx's error if the drain does not finish
// in time, nil otherwise.
func (s *Server) Stop(ctx context.Context) error {
	s.logger.Info("Shutting down the HTTP server")

	if err := s.svr.Shutdown(ctx); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}

	return nil
}
