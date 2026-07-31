package kms

import (
	"github.com/prometheus/client_golang/prometheus"

	"github.com/temporalio/temporal-proxy/internal/metrics"
	"github.com/temporalio/temporal-proxy/pkg/crypto"
)

type (
	// Reporter records encryption telemetry to Prometheus: KEK wrap/unwrap
	// calls, the AES-step duration of DEK operations, and DEK cache behavior. It
	// implements crypto.Observer so a Vault can notify it of those events, and
	// exposes KEKOp for the KEK decorator. The end-to-end envelope operation is
	// owned by internal/proxy, which times its own Seal and Open calls and
	// labels them by namespace. Handles for the low-cardinality label
	// combinations are pre-resolved so the emit path is a lock-free map read; an
	// unexpected combination falls back to WithLabelValues. A Reporter is safe
	// for concurrent use.
	Reporter struct {
		kekOps       *prometheus.CounterVec
		kekOpDur     *prometheus.HistogramVec
		cacheHits    prometheus.Counter
		cacheMisses  prometheus.Counter
		cacheSize    prometheus.Gauge
		dekOps       *prometheus.CounterVec
		dekOpDur     *prometheus.HistogramVec
		dekRotations *prometheus.CounterVec

		kekOpHandle       map[kekOpKey]prometheus.Counter
		kekDurHandle      map[kekDurKey]prometheus.Observer
		dekOpHandle       map[dekOpKey]prometheus.Counter
		dekDurHandle      map[string]prometheus.Observer
		dekRotationHandle map[string]prometheus.Counter
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

	dekOpKey struct {
		operation string
		result    string
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

	dekOpDur := f.NewHistogram(prometheus.HistogramOpts{
		Name: "dek_ops_duration_secs",
		Help: "Duration of the AES-256-GCM step alone in seconds, excluding any KEK wrap or unwrap, labeled by operation.",
		// Explicit buckets because the Prometheus defaults start at 5ms, which
		// would put nearly every observation in the first bucket: this measures
		// symmetric encryption of a payload, not a KMS round trip. Spans 10us to
		// 41ms. kek_ops_duration_secs keeps the defaults for the opposite reason.
		Buckets: prometheus.ExponentialBuckets(0.00001, 4, 7),
	}, []string{"operation"})

	dekOps := f.NewCounter(prometheus.CounterOpts{
		Name: "dek_ops_total",
		Help: "Total DEK operations (payload encrypt/decrypt), labeled by operation and result. The result is the AES-256-GCM step's own outcome, not the surrounding envelope operation's.",
	}, []string{"operation", "result"})

	dekRotations := f.NewCounter(prometheus.CounterOpts{
		Name: "dek_rotations_total",
		Help: "Total DEK rotations, labeled by reason. A rising on_demand rate means Refresh is falling behind.",
	}, []string{"reason"})

	r := &Reporter{
		kekOps:       kekOps,
		kekOpDur:     kekOpDur,
		dekOps:       dekOps,
		dekOpDur:     dekOpDur,
		dekRotations: dekRotations,
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
		kekOpHandle:       make(map[kekOpKey]prometheus.Counter),
		kekDurHandle:      make(map[kekDurKey]prometheus.Observer),
		dekOpHandle:       make(map[dekOpKey]prometheus.Counter),
		dekDurHandle:      make(map[string]prometheus.Observer),
		dekRotationHandle: make(map[string]prometheus.Counter),
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

	for _, op := range []string{"encrypt", "decrypt"} {
		r.dekDurHandle[op] = dekOpDur.WithLabelValues(op)
		for _, res := range results {
			r.dekOpHandle[dekOpKey{op, res}] = dekOps.WithLabelValues(op, res)
		}
	}

	for _, reason := range []string{"scheduled", "on_demand", "initial"} {
		r.dekRotationHandle[reason] = dekRotations.WithLabelValues(reason)
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

// Observe records e against the metric its type owns. There is no default
// case: an event type this switch does not recognize means this binary's
// pkg/crypto is newer than this Reporter. Dropping it is deliberate, not an
// oversight. Metric emission must never alter behaviour, so panicking or
// returning an error on an unrecognized event is not available here; a
// missing metric is a missing signal, not corrupted data.
func (r *Reporter) Observe(e crypto.Event) {
	switch ev := e.(type) {
	case crypto.EnvelopeEvent:
		r.envelopeOp(ev)
	case crypto.CacheEvent:
		r.cacheAccess(ev)
	case crypto.RotationEvent:
		r.rotated(ev)
	}
}

// envelopeOp records the AES-256-GCM portion of one envelope operation: its
// duration and, via dek_ops_total, its own result.
//
// Total and Namespace are deliberately unused. internal/proxy already records
// the end-to-end duration and operation counts, labeled by namespace, around
// its own Seal and Open calls as vault_ops_duration_secs and vault_ops_total;
// recording them here would duplicate those series and collide with them on
// the shared "encryption" subsystem. Err is likewise unused here for a
// reason, not an oversight: the envelope result already lives on
// internal/proxy's vault_ops_total. The result label instead comes from
// CryptoErr, the AES step's own outcome, deliberately not Err: a Seal that
// encrypts successfully and then fails to wrap its DEK is a KEK failure that
// kek_ops_total already reports, and counting it here would blame the wrong
// actor.
func (r *Reporter) envelopeOp(e crypto.EnvelopeEvent) {
	// No AES step, no DEK operation to record. CryptoAttempted is the only sound
	// test for that: a zero Crypto cannot distinguish a step that never ran from
	// one that finished inside the clock's resolution, so treating a zero as
	// "never ran" would undercount both fast successes and fast failures.
	if !e.CryptoAttempted {
		return
	}

	// Every observation from here is a real measurement, including a zero, which
	// belongs in the lowest bucket rather than being withheld.
	op := e.Op.String()
	r.countDEKOp(op, resultLabel(e.CryptoErr))
	r.observeDEKDur(op, e.Crypto.Seconds())
}

// cacheAccess records a DEK cache hit or miss, distinguished by e.Hit, and
// updates the cache-size gauge.
func (r *Reporter) cacheAccess(e crypto.CacheEvent) {
	if e.Hit {
		r.cacheHits.Inc()
	} else {
		r.cacheMisses.Inc()
	}

	r.cacheSize.Set(float64(e.Size))
}

// rotated counts a DEK rotation by reason. The namespace is deliberately
// unused, for the same cardinality reason the cache metrics carry no namespace
// label.
func (r *Reporter) rotated(e crypto.RotationEvent) {
	reason := e.Reason.String()
	if c, ok := r.dekRotationHandle[reason]; ok {
		c.Inc()
		return
	}

	r.dekRotations.WithLabelValues(reason).Inc()
}

// countDEKOp increments dek_ops_total for op and result, using the
// pre-resolved handle when the combination is one of the four expected ones
// and falling back to WithLabelValues otherwise.
func (r *Reporter) countDEKOp(op, result string) {
	if c, ok := r.dekOpHandle[dekOpKey{op, result}]; ok {
		c.Inc()
		return
	}

	r.dekOps.WithLabelValues(op, result).Inc()
}

// observeDEKDur records seconds against dek_ops_duration_secs for op, using
// the pre-resolved handle when op is one of the two expected operations and
// falling back to WithLabelValues otherwise.
func (r *Reporter) observeDEKDur(op string, seconds float64) {
	if o, ok := r.dekDurHandle[op]; ok {
		o.Observe(seconds)
		return
	}

	r.dekOpDur.WithLabelValues(op).Observe(seconds)
}
