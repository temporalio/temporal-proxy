package codecserver

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
	// there are no long-poll methods here, so a short budget is safe.
	readHeaderTimeout = 5 * time.Second
	readTimeout       = 10 * time.Second
)

// Server serves the codec server routes over HTTP on its own port, separate
// from the gateway's.
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

// Addr is the address the server is accepting on, or nil before [Server.Start]
// returns successfully. It is readable at all because the listener is bound
// explicitly rather than by ListenAndServe, which never reports the port it
// chose; that is what lets a caller bind port zero and still find the server.
//
// Safe for concurrent use.
func (s *Server) Addr() net.Addr {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.addr
}

// Start binds the listener and serves in a background goroutine, returning as
// soon as the listener is accepting. Serving continues until [Server.Stop],
// so Start does not block.
//
// Returns an error only if the bind fails, typically an address already in use.
// A failure after Start returns cannot be reported through it, so an unexpected
// stop reaches the abort function given to [NewServer] instead.
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
	log.Info("Starting the codec server")

	go func() {
		// ServeTLS with empty paths uses the certificates already on TLSConfig.
		serve := s.svr.Serve
		if s.svr.TLSConfig != nil {
			serve = func(l net.Listener) error { return s.svr.ServeTLS(l, "", "") }
		}

		if err := serve(lis); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("Codec server stopped serving", tag.Error(err))
			if s.abort != nil {
				s.abort(err)
			}
		}
	}()

	return nil
}

// Stop closes the listener and waits for in-flight requests to finish. A
// graceful stop is not an error, so the serving goroutine's abort function is
// not called.
//
// Returns ctx's error if the drain does not finish in time, nil otherwise.
func (s *Server) Stop(ctx context.Context) error {
	s.logger.Info("Shutting down the codec server")

	if err := s.svr.Shutdown(ctx); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}

	return nil
}
