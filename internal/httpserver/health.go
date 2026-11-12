package httpserver

import (
	"net/http"
)

// serveHealth answers the listener's own health route, for a load balancer or
// probe that holds no credential for any route group. A 200 proves only that
// this listener accepted the connection and ran a handler. Nothing else is
// consulted, the gateway included, so a dependency failing cannot take every
// replica out of rotation at once.
//
// The method is checked here rather than in the route's pattern. A request
// that misses a method-specific pattern falls through to whichever group is
// mounted at the root, so its answer would depend on which groups are enabled.
func serveHealth(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		w.Header().Set("Allow", "GET, HEAD")
		http.Error(w, http.StatusText(http.StatusMethodNotAllowed), http.StatusMethodNotAllowed)

		return
	}

	// A probe answered from a cache says nothing about this listener.
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	_, _ = w.Write([]byte("ok\n"))
}
