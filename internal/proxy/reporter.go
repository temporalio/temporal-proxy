package proxy

import (
	"context"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/temporalio/temporal-proxy/internal/metrics"
)

type (
	// Reporter records envelope-operation telemetry to Prometheus: each seal
	// (encrypt) and open (decrypt) the encryption interceptor performs, timed end
	// to end, including any KEK wrap or unwrap and any DEK cache lookup along the
	// way. The AES-step duration alone is owned by internal/kms. The namespace
	// label is always declared but only carries a value when namespace labels are
	// enabled, since the set of namespaces is unbounded; handles are resolved per
	// call via WithLabelValues rather than pre-computed. A Reporter is safe for
	// concurrent use.
	Reporter struct {
		ops             *prometheus.CounterVec
		duration        *prometheus.HistogramVec
		namespaceLabels bool
		labels          metrics.MetadataLabels
	}

	ReporterOption func(*Reporter)
)

// NewReporter builds the Prometheus-backed vault-operation Reporter. f must
// already be scoped to the "encryption" subsystem by the caller.
func NewReporter(f *metrics.Factory, opts ...ReporterOption) *Reporter {
	r := &Reporter{}
	for _, opt := range opts {
		opt(r)
	}

	r.ops = f.NewCounter(prometheus.CounterOpts{
		Name: "vault_ops_total",
		Help: "Total envelope operations (encrypt/decrypt), labeled by operation, result, and namespace.",
	}, append([]string{"operation", "result", "namespace"}, r.labels.Names()...))

	r.duration = f.NewHistogram(prometheus.HistogramOpts{
		Name: "vault_ops_duration_seconds",
		Help: "Duration of envelope operations in seconds end to end, including any KEK wrap or unwrap, labeled by operation and namespace.",
	}, append([]string{"operation", "namespace"}, r.labels.Names()...))

	return r
}

func WithNamespaceLabels(enabled bool) ReporterOption {
	return func(r *Reporter) { r.namespaceLabels = enabled }
}

func WithMetadataLabels(labels metrics.MetadataLabels) ReporterOption {
	return func(r *Reporter) { r.labels = labels }
}

// VaultOp records a single envelope operation and its duration. ctx is the
// request's, and supplies the configured metadata label values when there are
// any: on the per-upstream hop that is what the gateway forwarded rather than
// what it received, so a header the inbound authenticator consumed is gone,
// and on the codec server's HTTP path there is no gRPC metadata at all, so
// those labels come out blank there.
func (r *Reporter) VaultOp(ctx context.Context, operation, result, namespace string, seconds float64) {
	ns := r.nsLabel(namespace)
	labels := r.labels.AppendValues(ctx, nil)
	r.ops.WithLabelValues(append([]string{operation, result, ns}, labels...)...).Inc()
	r.duration.WithLabelValues(append([]string{operation, ns}, labels...)...).Observe(seconds)
}

// nsLabel returns ns when namespace labels are enabled and blank when they are
// not, which Prometheus reads as the label not being there.
func (r *Reporter) nsLabel(ns string) string {
	if r.namespaceLabels {
		return ns
	}

	return ""
}
