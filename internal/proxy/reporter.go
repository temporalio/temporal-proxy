package proxy

import (
	"github.com/prometheus/client_golang/prometheus"

	"github.com/temporalio/temporal-proxy/internal/metrics"
)

// Reporter records envelope-operation telemetry to Prometheus: each seal
// (encrypt) and open (decrypt) the encryption interceptor performs, timed end
// to end, including any KEK wrap or unwrap and any DEK cache lookup along the
// way. The AES-step duration alone is owned by internal/kms. The namespace
// label is always declared but only carries a value when namespace labels are
// enabled, since the set of namespaces is unbounded; handles are resolved per
// call via WithLabelValues rather than pre-computed. A Reporter is safe for
// concurrent use.
type Reporter struct {
	ops             *prometheus.CounterVec
	duration        *prometheus.HistogramVec
	namespaceLabels bool
}

// NewReporter builds the Prometheus-backed vault-operation Reporter. f must
// already be scoped to the "encryption" subsystem by the caller, and
// namespaceLabels decides whether the namespace label carries a value.
func NewReporter(f *metrics.Factory, namespaceLabels bool) *Reporter {
	return &Reporter{
		ops: f.NewCounter(prometheus.CounterOpts{
			Name: "vault_ops_total",
			Help: "Total envelope operations (encrypt/decrypt), labeled by operation, result, and namespace.",
		}, []string{"operation", "result", "namespace"}),
		duration: f.NewHistogram(prometheus.HistogramOpts{
			Name: "vault_ops_duration_seconds",
			Help: "Duration of envelope operations in seconds end to end, including any KEK wrap or unwrap, labeled by operation and namespace.",
		}, []string{"operation", "namespace"}),
		namespaceLabels: namespaceLabels,
	}
}

// VaultOp records a single envelope operation and its duration.
func (r *Reporter) VaultOp(operation, result, namespace string, seconds float64) {
	ns := r.nsLabel(namespace)
	r.ops.WithLabelValues(operation, result, ns).Inc()
	r.duration.WithLabelValues(operation, ns).Observe(seconds)
}

// nsLabel returns ns when namespace labels are enabled and blank when they are
// not, which Prometheus reads as the label not being there.
func (r *Reporter) nsLabel(ns string) string {
	if r.namespaceLabels {
		return ns
	}

	return ""
}
