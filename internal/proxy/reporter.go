package proxy

import (
	"context"

	"github.com/prometheus/client_golang/prometheus"

	"go.temporal.io/proxy/internal/metrics"
)

type (
	// Reporter records envelope-operation telemetry to Prometheus: each seal
	// (encrypt) and open (decrypt) the encryption codec performs, on both the
	// gRPC path and the codec server's HTTP path, timed end to end, including
	// any KEK wrap or unwrap and any DEK cache lookup along the way. It also
	// counts payloads the encryption codec skipped, payloads opened without a
	// namespace binding, and payloads bound to a namespace other than the
	// request's. The AES-step duration alone is owned by internal/kms. The
	// namespace label is always declared but only carries a value when namespace
	// labels are enabled, since the set of namespaces is unbounded; handles are
	// resolved per call via WithLabelValues rather than pre-computed. A Reporter is
	// safe for concurrent use.
	Reporter struct {
		reporterLabels

		ops        *prometheus.CounterVec
		duration   *prometheus.HistogramVec
		skipped    *prometheus.CounterVec
		unbound    *prometheus.CounterVec
		mismatches *prometheus.CounterVec
	}

	// CompressionReporter records compression telemetry to Prometheus: each
	// payload the compression codec compresses or restores, on both the gRPC path
	// and the codec server's HTTP path, the sizes either side of compressing it,
	// and each payload it forwards uncompressed. The namespace label behaves as it
	// does on [Reporter]. A CompressionReporter is safe for concurrent use.
	CompressionReporter struct {
		reporterLabels

		ops      *prometheus.CounterVec
		duration *prometheus.HistogramVec
		bytes    *prometheus.HistogramVec
		skipped  *prometheus.CounterVec
	}

	// ReporterOption configures a [Reporter] or [CompressionReporter] at
	// construction.
	ReporterOption func(*reporterLabels)

	// reporterLabels is how a request-scoped reporter labels its series beyond
	// its own labels.
	reporterLabels struct {
		namespaceLabels bool
		labels          metrics.MetadataLabels
	}
)

// NewReporter builds the Prometheus-backed vault-operation Reporter. f must
// already be scoped to the "encryption" subsystem by the caller.
func NewReporter(f *metrics.Factory, opts ...ReporterOption) *Reporter {
	r := &Reporter{}
	for _, opt := range opts {
		opt(&r.reporterLabels)
	}

	r.ops = f.NewCounter(prometheus.CounterOpts{
		Name: "vault_ops_total",
		Help: "Total envelope operations (encrypt/decrypt), labeled by operation, result, and namespace.",
	}, append([]string{"operation", "result", "namespace"}, r.labels.Names()...))

	r.duration = f.NewHistogram(prometheus.HistogramOpts{
		Name: "vault_ops_duration_seconds",
		Help: "Duration of envelope operations in seconds end to end, including any KEK wrap or unwrap, labeled by operation and namespace.",
	}, append([]string{"operation", "namespace"}, r.labels.Names()...))

	r.skipped = f.NewCounter(prometheus.CounterOpts{
		Name: "payloads_skipped_total",
		Help: "Total payloads the encryption codec left as they were, labeled by operation, encoding, and namespace.",
	}, append([]string{"operation", "encoding", "namespace"}, r.labels.Names()...))

	r.unbound = f.NewCounter(prometheus.CounterOpts{
		Name: "vault_unbound_opens_total",
		Help: "Total payloads handed to decrypt that are not bound to a namespace, whatever the outcome, labeled by namespace.",
	}, append([]string{"namespace"}, r.labels.Names()...))

	r.mismatches = f.NewCounter(prometheus.CounterOpts{
		Name: "vault_namespace_mismatches_total",
		Help: "Total payloads handed to decrypt that are bound to a namespace other than the request's, whatever the outcome, labeled by namespace.",
	}, append([]string{"namespace"}, r.labels.Names()...))

	return r
}

// NewCompressionReporter builds the Prometheus-backed compression Reporter. f
// must already be scoped to the "compression" subsystem by the caller.
func NewCompressionReporter(f *metrics.Factory, opts ...ReporterOption) *CompressionReporter {
	r := &CompressionReporter{}
	for _, opt := range opts {
		opt(&r.reporterLabels)
	}

	r.ops = f.NewCounter(prometheus.CounterOpts{
		Name: "ops_total",
		Help: "Total payloads compressed or decompressed, labeled by operation, algorithm, result, and namespace.",
	}, append([]string{"operation", "algorithm", "result", "namespace"}, r.labels.Names()...))

	r.duration = f.NewHistogram(prometheus.HistogramOpts{
		Name: "ops_duration_seconds",
		Help: "Duration of compressing or decompressing one payload in seconds, labeled by operation, algorithm, and namespace.",
		// Explicit buckets because the Prometheus defaults start at 5ms, and one
		// payload takes microseconds. Spans 10us to 164ms.
		Buckets: prometheus.ExponentialBuckets(0.00001, 4, 8),
	}, append([]string{"operation", "algorithm", "namespace"}, r.labels.Names()...))

	r.bytes = f.NewHistogram(prometheus.HistogramOpts{
		Name: "payload_bytes",
		Help: "Size in bytes of each payload compressed, before and after, labeled by stage, algorithm, and namespace.",
		// Spans 64B to 4MiB, the largest payload the gateway accepts.
		Buckets: prometheus.ExponentialBuckets(64, 4, 9),
	}, append([]string{"stage", "algorithm", "namespace"}, r.labels.Names()...))

	r.skipped = f.NewCounter(prometheus.CounterOpts{
		Name: "payloads_skipped_total",
		Help: "Total payloads forwarded uncompressed while compression is on, labeled by reason, algorithm, and namespace.",
	}, append([]string{"reason", "algorithm", "namespace"}, r.labels.Names()...))

	return r
}

// WithNamespaceLabels sets whether the namespace label carries a value. Off by
// default, since the set of namespaces is unbounded.
func WithNamespaceLabels(enabled bool) ReporterOption {
	return func(r *reporterLabels) { r.namespaceLabels = enabled }
}

// WithMetadataLabels sets the request-metadata headers reported as extra labels.
func WithMetadataLabels(labels metrics.MetadataLabels) ReporterOption {
	return func(r *reporterLabels) { r.labels = labels }
}

// VaultOp records a single envelope operation and its duration. ctx is the
// request's, and supplies the configured metadata label values when there are
// any: on the gRPC path that is the incoming metadata after the inbound
// authenticator stripped the headers it consumed, so those headers are gone,
// and on the codec server's HTTP path there is no gRPC metadata at all, so
// those labels come out blank there.
func (r *Reporter) VaultOp(ctx context.Context, operation, result, namespace string, seconds float64) {
	ns := r.nsLabel(namespace)
	labels := r.labels.AppendValues(ctx, nil)
	r.ops.WithLabelValues(append([]string{operation, result, ns}, labels...)...).Inc()
	r.duration.WithLabelValues(append([]string{operation, ns}, labels...)...).Observe(seconds)
}

// PayloadSkipped records one payload the encryption codec forwarded without
// sealing or opening it. encoding only ever holds a configured skipEncodings
// entry, so it stays bounded.
func (r *Reporter) PayloadSkipped(ctx context.Context, operation, encoding, namespace string) {
	labels := r.labels.AppendValues(ctx, nil)
	r.skipped.WithLabelValues(append([]string{operation, encoding, r.nsLabel(namespace)}, labels...)...).Inc()
}

// UnboundOpen records one payload handed to decrypt that is not bound to a
// namespace, before the vault decides what to do with it. Under a permissive
// binding mode it counts what strict would refuse, so a namespace that stays at
// zero for longer than its retention is ready for strict.
func (r *Reporter) UnboundOpen(ctx context.Context, namespace string) {
	labels := r.labels.AppendValues(ctx, nil)
	r.unbound.WithLabelValues(append([]string{r.nsLabel(namespace)}, labels...)...).Inc()
}

// NamespaceMismatch records one payload handed to decrypt that is bound to a
// namespace other than the request's, before the vault decides what to do with
// it. Under a permissive binding mode it counts what strict would refuse.
func (r *Reporter) NamespaceMismatch(ctx context.Context, namespace string) {
	labels := r.labels.AppendValues(ctx, nil)
	r.mismatches.WithLabelValues(append([]string{r.nsLabel(namespace)}, labels...)...).Inc()
}

// Op records one payload compressed or decompressed and how long it took. As
// with [Reporter.VaultOp], metadata labels come out blank on the codec server's
// HTTP path.
func (r *CompressionReporter) Op(ctx context.Context, operation, algorithm, result, namespace string, seconds float64) {
	ns := r.nsLabel(namespace)
	labels := r.labels.AppendValues(ctx, nil)
	r.ops.WithLabelValues(append([]string{operation, algorithm, result, ns}, labels...)...).Inc()
	r.duration.WithLabelValues(append([]string{operation, algorithm, ns}, labels...)...).Observe(seconds)
}

// PayloadBytes records the size of one payload at stage, pre_compress or
// post_compress.
func (r *CompressionReporter) PayloadBytes(ctx context.Context, stage, algorithm, namespace string, n int) {
	labels := r.labels.AppendValues(ctx, nil)
	r.bytes.WithLabelValues(append([]string{stage, algorithm, r.nsLabel(namespace)}, labels...)...).Observe(float64(n))
}

// PayloadSkipped records one payload forwarded uncompressed, and why.
func (r *CompressionReporter) PayloadSkipped(ctx context.Context, reason, algorithm, namespace string) {
	labels := r.labels.AppendValues(ctx, nil)
	r.skipped.WithLabelValues(append([]string{reason, algorithm, r.nsLabel(namespace)}, labels...)...).Inc()
}

// nsLabel returns ns when namespace labels are enabled and blank when they are
// not, which Prometheus reads as the label not being there.
func (r *reporterLabels) nsLabel(ns string) string {
	if r.namespaceLabels {
		return ns
	}

	return ""
}
