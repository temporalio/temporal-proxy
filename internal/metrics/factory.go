package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Factory binds a Prometheus namespace (and optionally a subsystem) to a
// promauto.Factory so callers create collectors that are automatically
// namespaced and registered, without each call site repeating the prefix. It is
// the dependency other packages inject to declare their own metrics.
type Factory struct {
	namespace string // Prometheus namespace prefixed onto every collector.
	subsystem string // Optional subsystem, prefixed after the namespace.
	factory   promauto.Factory
}

// New returns a Factory that prefixes ns onto every collector it creates, using
// factory to build and register them with the underlying registry.
func New(ns string, factory promauto.Factory) *Factory {
	return &Factory{
		namespace: ns,
		factory:   factory,
	}
}

// ForSubsystem returns a copy of the Factory scoped to subsys, so collectors it
// creates are named namespace_subsys_<name>. The receiver is left unchanged, so
// one namespace-level Factory can spawn independent per-subsystem factories.
func (f *Factory) ForSubsystem(subsys string) *Factory {
	return &Factory{
		namespace: f.namespace,
		subsystem: subsys,
		factory:   f.factory,
	}
}

// NewCounter creates and registers a CounterVec. It forces the bound namespace
// (and subsystem, when set) onto opts, overriding any the caller set, so every
// counter shares the same prefix. labelNames are the counter's variable label
// dimensions.
func (f *Factory) NewCounter(opts prometheus.CounterOpts, labelNames []string) *prometheus.CounterVec {
	opts.Namespace = f.namespace
	if f.subsystem != "" {
		opts.Subsystem = f.subsystem
	}

	return f.factory.NewCounterVec(opts, labelNames)
}
