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
// every other response.
//
// It wraps the router rather than sitting inside a route, for two reasons. A
// preflight arrives as OPTIONS, which matches no route and would otherwise
// answer 405, and it has to be answered before authentication because a
// browser sends no credentials on one.
//
// The request's own origin is echoed back, and only when it is on the allowed
// list. A wildcard is never emitted, since a browser rejects one whenever
// credentials are included.
//
// Returns next unchanged when origins is empty, otherwise a handler wrapping it.
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
