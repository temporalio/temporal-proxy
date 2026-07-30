package connect

import "go.uber.org/fx"

// Module provides a *Pool and binds its lifecycle to the application, closing
// every pooled connection on shutdown via an fx stop hook.
//
// The pool is deliberately not opened as a whole on start. It also holds the
// router's loopback connections to sockets this application binds itself, which
// do not exist until the proxy start hooks run; waiting on those here would
// block on work a later hook has yet to do. Opening eager connections is the job
// of whoever owns one, through [WaitReady].
var Module = fx.Options(
	fx.Provide(NewPool),
	fx.Invoke(func(p *Pool, lc fx.Lifecycle) {
		lc.Append(fx.StopHook(p.Close))
	}),
)
