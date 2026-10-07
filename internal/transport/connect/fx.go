package connect

import "go.uber.org/fx"

// Module provides a *Pool and binds its lifecycle to the application, closing
// every pooled connection on shutdown via an fx stop hook.
//
// The pool is deliberately not opened as a whole on start. It also holds
// connections that are lazy on purpose, such as a templated upstream's, created
// per request, and the Cloud control plane's, opened on first use; waiting on
// those here would hold up startup for connections nothing needs yet. Opening
// eager connections is the job of whoever owns one, through [WaitReady].
var Module = fx.Options(
	fx.Provide(NewPool),
	fx.Invoke(func(p *Pool, lc fx.Lifecycle) {
		lc.Append(fx.StopHook(p.Close))
	}),
)
