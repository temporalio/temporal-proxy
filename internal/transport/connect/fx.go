package connect

import "go.uber.org/fx"

// Module provides a *Pool and binds its lifecycle to the application, closing
// every pooled connection on shutdown via an fx stop hook.
var Module = fx.Options(
	fx.Provide(NewPool),
	fx.Invoke(func(p *Pool, lc fx.Lifecycle) {
		lc.Append(fx.StopHook(func() error {
			return p.Close()
		}))
	}),
)
