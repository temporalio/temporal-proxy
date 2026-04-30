package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
)

type (
	// Gauge records observations for a registered gauge metric.
	Gauge struct {
		name   string
		vec    *prometheus.GaugeVec
		labels []string
		logger log.Logger
	}

	// Counter records observations for a registered counter metric.
	Counter struct {
		name   string
		vec    *prometheus.CounterVec
		labels []string
		logger log.Logger
	}

	// Histogram records observations for a registered histogram metric.
	Histogram struct {
		name   string
		vec    *prometheus.HistogramVec
		labels []string
		logger log.Logger
	}
)

// Inc increments the gauge by 1. kvs are key-value label pairs (e.g. "cluster", "foo").
// Invalid or mismatched labels are logged as a warning and the call is a no-op.
func (h *Gauge) Inc(kvs ...string) {
	lbls, ok := buildLabels(h.labels, kvs)
	if !ok {
		logInvalidLabels(h.logger, h.name)
		return
	}

	h.vec.With(lbls).Inc()
}

// Dec decrements the gauge by 1. kvs are key-value label pairs.
// Invalid or mismatched labels are logged as a warning and the call is a no-op.
func (h *Gauge) Dec(kvs ...string) {
	lbls, ok := buildLabels(h.labels, kvs)
	if !ok {
		logInvalidLabels(h.logger, h.name)
		return
	}

	h.vec.With(lbls).Dec()
}

// Set sets the gauge to val. kvs are key-value label pairs.
// Invalid or mismatched labels are logged as a warning and the call is a no-op.
func (h *Gauge) Set(val float64, kvs ...string) {
	lbls, ok := buildLabels(h.labels, kvs)
	if !ok {
		logInvalidLabels(h.logger, h.name)
		return
	}

	h.vec.With(lbls).Set(val)
}

// Add adds val to the gauge. kvs are key-value label pairs.
// Invalid or mismatched labels are logged as a warning and the call is a no-op.
func (h *Gauge) Add(val float64, kvs ...string) {
	lbls, ok := buildLabels(h.labels, kvs)
	if !ok {
		logInvalidLabels(h.logger, h.name)
		return
	}

	h.vec.With(lbls).Add(val)
}

// Inc increments the counter by 1. kvs are key-value label pairs (e.g. "cluster", "foo").
// Invalid or mismatched labels are logged as a warning and the call is a no-op.
func (h *Counter) Inc(kvs ...string) {
	lbls, ok := buildLabels(h.labels, kvs)
	if !ok {
		logInvalidLabels(h.logger, h.name)
		return
	}

	h.vec.With(lbls).Inc()
}

// Add adds val to the counter. kvs are key-value label pairs.
// Invalid or mismatched labels are logged as a warning and the call is a no-op.
func (h *Counter) Add(val float64, kvs ...string) {
	lbls, ok := buildLabels(h.labels, kvs)
	if !ok {
		logInvalidLabels(h.logger, h.name)
		return
	}

	h.vec.With(lbls).Add(val)
}

// Observe records val as a histogram observation. kvs are key-value label pairs (e.g. "cluster", "foo").
// Invalid or mismatched labels are logged as a warning and the call is a no-op.
func (h *Histogram) Observe(val float64, kvs ...string) {
	lbls, ok := buildLabels(h.labels, kvs)
	if !ok {
		logInvalidLabels(h.logger, h.name)
		return
	}

	h.vec.With(lbls).Observe(val)
}

// buildLabels builds a prometheus.Labels map from key-value pairs and validates them
// against the registered label names. It returns false (and callers no-op + warn) rather
// than panicking or propagating an error: metrics are observability infrastructure and
// must never affect the application being observed. A missing data point is preferable
// to a crash or error cascade; label mismatches are programming errors that should
// surface in tests and logs, not in production control flow.
func buildLabels(registered []string, kvs []string) (prometheus.Labels, bool) {
	if len(kvs)%2 != 0 {
		return nil, false
	}
	if len(kvs)/2 != len(registered) {
		return nil, false
	}

	labels := make(prometheus.Labels, len(registered))
	for i := 0; i < len(kvs); i += 2 {
		labels[kvs[i]] = kvs[i+1]
	}

	for _, name := range registered {
		if _, ok := labels[name]; !ok {
			return nil, false
		}
	}

	return labels, true
}

func logInvalidLabels(l log.Logger, name string) {
	l.Warn("metrics: invalid labels", tag.String("metric", name))
}
