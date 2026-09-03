// Package metrics wires Prometheus metrics into the proxy. Its fx [Module]
// provides a namespaced [Factory] bound to the injected Prometheus registry and
// serves that registry at /metrics over HTTP, binding the server to the fx
// application lifecycle. Consumers inject the [Factory], optionally scoping it
// to a subsystem with [Factory.ForSubsystem], to declare their collectors.
//
// A reporter emitting while a request is in flight also takes a [Tags], built
// from the configured header-to-label pairs, and carries those labels on its
// collectors. Values are read from the request's incoming metadata at each emit
// rather than resolved once and carried along, because the proxy forwards over a
// socket that context values do not cross while metadata does.
package metrics
