package metrics

import (
	"errors"

	"github.com/prometheus/client_golang/prometheus"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
)

type (
	// Provider manages a set of Prometheus metrics against an injected registerer.
	Provider struct {
		logger     log.Logger
		registerer prometheus.Registerer
	}

	// ProviderOption configures a Provider.
	ProviderOption func(*Provider)
)

// NewProvider returns a Provider. By default it uses prometheus.DefaultRegisterer and a CLI logger.
func NewProvider(opts ...ProviderOption) *Provider {
	p := &Provider{
		logger:     log.NewCLILogger(),
		registerer: prometheus.DefaultRegisterer,
	}

	for _, opt := range opts {
		opt(p)
	}

	return p
}

// Logger sets the logger used for duplicate-registration warnings. Defaults to a CLI logger.
func Logger(l log.Logger) ProviderOption {
	return func(p *Provider) { p.logger = l }
}

// Registerer sets the Prometheus registerer used to register metrics. Defaults to prometheus.DefaultRegisterer.
func Registerer(r prometheus.Registerer) ProviderOption {
	return func(p *Provider) { p.registerer = r }
}

// RegisterGauge registers a gauge metric and returns a handle for recording observations.
// If the metric is already registered, the existing registration is reused.
func (p *Provider) RegisterGauge(name, help string, labelNames ...string) *Gauge {
	vec := prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: name, Help: help}, labelNames)
	if err := p.registerer.Register(vec); err != nil {
		if are, ok := errors.AsType[prometheus.AlreadyRegisteredError](err); ok {
			p.logger.Warn("metrics: duplicate gauge registration", tag.NewStringTag("name", name))
			if existing, ok := are.ExistingCollector.(*prometheus.GaugeVec); ok {
				vec = existing
			} else {
				p.logger.Warn("metrics: existing collector has wrong type, gauge will be a no-op",
					tag.NewStringTag("name", name))
			}
		}
	}

	return &Gauge{name: name, vec: vec, labels: labelNames, logger: p.logger}
}

// RegisterCounter registers a counter metric and returns a handle for recording observations.
// If the metric is already registered, the existing registration is reused.
func (p *Provider) RegisterCounter(name, help string, labelNames ...string) *Counter {
	vec := prometheus.NewCounterVec(prometheus.CounterOpts{Name: name, Help: help}, labelNames)
	if err := p.registerer.Register(vec); err != nil {
		if are, ok := errors.AsType[prometheus.AlreadyRegisteredError](err); ok {
			p.logger.Warn("metrics: duplicate counter registration", tag.NewStringTag("name", name))
			if existing, ok := are.ExistingCollector.(*prometheus.CounterVec); ok {
				vec = existing
			} else {
				p.logger.Warn("metrics: existing collector has wrong type, counter will be a no-op",
					tag.NewStringTag("name", name))
			}
		}
	}

	return &Counter{name: name, vec: vec, labels: labelNames, logger: p.logger}
}

// RegisterHistogram registers a histogram metric and returns a handle for recording observations.
// If the metric is already registered, the existing registration is reused.
func (p *Provider) RegisterHistogram(name, help string, buckets []float64, labelNames ...string) *Histogram {
	vec := prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: name, Help: help, Buckets: buckets}, labelNames)
	if err := p.registerer.Register(vec); err != nil {
		if are, ok := errors.AsType[prometheus.AlreadyRegisteredError](err); ok {
			p.logger.Warn("metrics: duplicate histogram registration", tag.NewStringTag("name", name))
			if existing, ok := are.ExistingCollector.(*prometheus.HistogramVec); ok {
				vec = existing
			} else {
				p.logger.Warn("metrics: existing collector has wrong type, histogram will be a no-op",
					tag.NewStringTag("name", name))
			}
		}
	}

	return &Histogram{name: name, vec: vec, labels: labelNames, logger: p.logger}
}
