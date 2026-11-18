package pprofserver

import (
	"net/http"
	netpprof "net/http/pprof"

	"go.temporal.io/proxy/internal/auth"
	"go.temporal.io/proxy/internal/httpserver"
	"go.temporal.io/proxy/internal/transport/meta"
	"go.temporal.io/proxy/pkg/logger/tag"
)

// route is the URL prefix every profile route is mounted under.
const route = "/debug/pprof/"

// newFromParams builds the routes the configuration describes, or returns none
// when the pprof block is disabled. Authentication wraps the whole group when
// configured. The profiles name no namespace, so the namespace a decision names
// is always "".
func newFromParams(p Params) ([]httpserver.Route, error) {
	cfg := &p.Config.HTTP.PProf
	if !cfg.Enabled {
		return nil, nil
	}

	log := p.Logger.With(tag.Component("pprofserver"))

	authenticate := func(next http.Handler) http.Handler { return next }
	if cfg.Auth != nil {
		authn, err := auth.For(cfg.Auth, p.Conns)
		if err != nil {
			return nil, err
		}

		authenticate = auth.HTTPMiddleware(
			authn,
			meta.HTTPGroupPProf,
			func(*http.Request) string { return "" },
			log,
		)
	} else {
		// Configuration only permits this on a loopback bind.
		log.Warn("Pprof routes are running without authentication, which is only allowed on a loopback bind")
	}

	return []httpserver.Route{{Pattern: route, Handler: authenticate(newHandler())}}, nil
}

// newHandler builds the mux serving the standard profile endpoints under
// [route]. Mounting them on a mux of our own rather than letting
// net/http/pprof register its globals keeps the routes behind the group's
// authentication and off any other server in the process.
func newHandler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc(route, netpprof.Index)
	mux.HandleFunc(route+"cmdline", netpprof.Cmdline)
	mux.HandleFunc(route+"profile", netpprof.Profile)
	mux.HandleFunc(route+"symbol", netpprof.Symbol)
	mux.HandleFunc(route+"trace", netpprof.Trace)

	return mux
}
