// Package metrics wires Prometheus metrics into the proxy. Its fx [Module]
// provides a namespaced [Factory] bound to the injected Prometheus registry and
// serves that registry at /metrics over HTTP, binding the server to the fx
// application lifecycle. Consumers inject the [Factory], optionally scoping it
// to a subsystem with [Factory.ForSubsystem], to declare their collectors.
package metrics
