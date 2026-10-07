package connect

import "go.uber.org/fx"

// Module provides a *Pool and binds its lifecycle to the application, closing
// every pooled connection on shutdown via an fx stop hook. It does not open the
// pool on start; opening an eager connection is the job of whoever owns it,
// through [WaitReady].
var Module = fx.Options(
	fx.Provide(NewPool),
	fx.Invoke(func(p *Pool, lc fx.Lifecycle) {
		lc.Append(fx.StopHook(p.Close))
	}),
)
