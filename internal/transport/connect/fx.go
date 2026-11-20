package connect

import "go.uber.org/fx"

// Module provides a *Pool and binds its lifecycle to the application, closing
// every pooled connection on shutdown via an fx stop hook. It does not open the
// pool on start; opening an eager connection is the job of whoever owns it,
// through [WaitReady].
//
// The stop hook is registered when the pool is built, so it is registered
// before the hook of anything built from the pool, and fx, stopping hooks in
// reverse, closes the pool only after everything that uses it has stopped,
// whatever order the modules are listed in.
var Module = fx.Provide(newFromLifecycle)

// newFromLifecycle builds a Pool and registers its stop hook on lc.
func newFromLifecycle(lc fx.Lifecycle) *Pool {
	p := NewPool()
	lc.Append(fx.StopHook(p.Close))

	return p
}
