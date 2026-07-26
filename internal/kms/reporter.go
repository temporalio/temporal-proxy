package kms

import (
	"github.com/prometheus/client_golang/prometheus"

	"github.com/temporalio/temporal-proxy/internal/metrics"
	"github.com/temporalio/temporal-proxy/pkg/crypto"
)

type (
	// Reporter records encryption telemetry to Prometheus: KEK wrap/unwrap
	// calls and DEK cache behavior. It implements crypto.Observer so a Vault can
	// notify it of cache events, and exposes KEKOp for the KEK decorator. Handles
	// for the low-cardinality KEK label combinations are pre-resolved so the emit
	// path is a lock-free map read; an unexpected combination falls back to
	// WithLabelValues. A Reporter is safe for concurrent use.
	Reporter struct {
		kekOps      *prometheus.CounterVec
		kekOpDur    *prometheus.HistogramVec
		cacheHits   prometheus.Counter
		cacheMisses prometheus.Counter
		cacheSize   prometheus.Gauge

		kekOpHandle  map[kekOpKey]prometheus.Counter
		kekDurHandle map[kekDurKey]prometheus.Observer
	}

	kekOpKey struct {
		provider  string
		operation string
		result    string
	}

	kekDurKey struct {
		provider  string
		operation string
	}
)

// NewReporter builds the Prometheus-backed encryption Reporter, pre-resolving
// the meaningful KEK label combinations so every series starts at zero. f must
// already be scoped to the "encryption" subsystem by the caller.
func NewReporter(f *metrics.Factory) *Reporter {
	kekOps := f.NewCounter(prometheus.CounterOpts{
		Name: "kek_ops_total",
		Help: "Total KEK operations (DEK wrap/unwrap), labeled by provider, operation, and result.",
	}, []string{"provider", "operation", "result"})

	kekOpDur := f.NewHistogram(prometheus.HistogramOpts{
		Name: "kek_ops_duration_secs",
		Help: "Duration of KEK operations in seconds, labeled by provider and operation.",
	}, []string{"provider", "operation"})

	r := &Reporter{
		kekOps:   kekOps,
		kekOpDur: kekOpDur,
		cacheHits: f.NewCounter(prometheus.CounterOpts{
			Name: "dek_cache_hits_total",
			Help: "Total DEK cache hits.",
		}, nil).WithLabelValues(),
		cacheMisses: f.NewCounter(prometheus.CounterOpts{
			Name: "dek_cache_misses_total",
			Help: "Total DEK cache misses.",
		}, nil).WithLabelValues(),
		cacheSize: f.NewGauge(prometheus.GaugeOpts{
			Name: "dek_cache_size",
			Help: "Current number of entries in the DEK cache.",
		}),
		kekOpHandle:  make(map[kekOpKey]prometheus.Counter),
		kekDurHandle: make(map[kekDurKey]prometheus.Observer),
	}

	providers := []string{"aws", "gcp", "azure", "testing"}
	operations := []string{"wrap", "unwrap"}
	results := []string{"success", "error"}
	for _, p := range providers {
		for _, op := range operations {
			r.kekDurHandle[kekDurKey{p, op}] = kekOpDur.WithLabelValues(p, op)
			for _, res := range results {
				r.kekOpHandle[kekOpKey{p, op, res}] = kekOps.WithLabelValues(p, op, res)
			}
		}
	}

	return r
}

// KEKOp records a single KEK operation and its duration.
func (r *Reporter) KEKOp(provider, operation, result string, seconds float64) {
	if c, ok := r.kekOpHandle[kekOpKey{provider, operation, result}]; ok {
		c.Inc()
	} else {
		r.kekOps.WithLabelValues(provider, operation, result).Inc()
	}

	if o, ok := r.kekDurHandle[kekDurKey{provider, operation}]; ok {
		o.Observe(seconds)
	} else {
		r.kekOpDur.WithLabelValues(provider, operation).Observe(seconds)
	}
}

// CacheHit records a DEK cache hit and updates the cache-size gauge.
func (r *Reporter) CacheHit(e crypto.CacheEvent) {
	r.cacheHits.Inc()
	r.cacheSize.Set(float64(e.Size))
}

// CacheMiss records a DEK cache miss and updates the cache-size gauge.
func (r *Reporter) CacheMiss(e crypto.CacheEvent) {
	r.cacheMisses.Inc()
	r.cacheSize.Set(float64(e.Size))
}
