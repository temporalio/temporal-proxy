// Package metrics wires Prometheus metrics into the proxy. Its fx [Module]
// provides a promauto.Factory bound to the injected Prometheus registry and
// serves that registry at /metrics over HTTP, binding the server to the fx
// application lifecycle.
package metrics
