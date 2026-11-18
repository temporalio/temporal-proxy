package server

import "net"

// Listener is the listener Start bound, exported for tests that close it to
// drive an unexpected stop.
func (s *Server) Listener() net.Listener {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.lis
}
