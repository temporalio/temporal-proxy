package codecserver

import (
	"net/http"
)

// allowedHeaders is what a browser is told it may send. Authorization-Extras
// carries the ID token the Temporal UI sends alongside the access token when its
// pass-access-token setting is on, so omitting it fails the preflight whenever
// one is present.
const allowedHeaders = "Content-Type, X-Namespace, Authorization, Authorization-Extras"

// cors answers preflight requests and stamps the cross-origin headers onto
// every other response. It must wrap the router: a preflight arrives as
// OPTIONS, matches no route, and carries no credentials, so it is answered
// before routing and authentication. An allowed origin is echoed back; a
// wildcard is never emitted, since a browser rejects one whenever credentials
// are included. Returns next unchanged when origins is empty.
func cors(origins []string, credentials bool, next http.Handler) http.Handler {
	if len(origins) == 0 {
		return next
	}

	allowed := make(map[string]struct{}, len(origins))
	for _, origin := range origins {
		allowed[origin] = struct{}{}
	}

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Set unconditionally, not only on an allowed origin: a shared cache that
		// stores the header-less response for a disallowed origin must not replay
		// it to a request from an allowed one.
		w.Header().Add("Vary", "Origin")

		origin := r.Header.Get("Origin")
		if _, ok := allowed[origin]; ok {
			w.Header().Set("Access-Control-Allow-Origin", origin)
			w.Header().Set("Access-Control-Allow-Methods", "POST, OPTIONS")
			w.Header().Set("Access-Control-Allow-Headers", allowedHeaders)
			if credentials {
				w.Header().Set("Access-Control-Allow-Credentials", "true")
			}
		}

		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)

			return
		}

		next.ServeHTTP(w, r)
	})
}
