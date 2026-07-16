package router

import (
	"github.com/prometheus/client_golang/prometheus"

	"github.com/temporalio/temporal-proxy/internal/metrics"
)

const (
	// upstreamUnknown is the upstream label used for unroutable decisions, where
	// no upstream was chosen.
	upstreamUnknown = "unknown"

	reasonNoConnection = "no_connection"
	reasonStreamSetup  = "stream_setup"
)

type (
	// Reporter records router telemetry to Prometheus: routing decisions and the
	// forwarding failures the router itself originates. It pre-resolves a counter
	// for every meaningful (upstream, outcome) and (upstream, reason) combination
	// so the emit path is a lock-free map read; an unexpected label combination
	// falls back to CounterVec.WithLabelValues. A Reporter is safe for concurrent
	// use.
	Reporter struct {
		decisions *prometheus.CounterVec
		errors    *prometheus.CounterVec
		decHandle map[decisionKey]prometheus.Counter
		errHandle map[errorKey]prometheus.Counter
	}

	decisionKey struct {
		upstream string
		outcome  Outcome
	}

	errorKey struct {
		upstream string
		reason   string
	}
)

// NewReporter builds the Prometheus-backed Reporter, registering its collectors
// with the factory's registry and pre-resolving the meaningful label
// combinations so every series starts at zero. upstreams is the configured
// upstream name list.
func NewReporter(f *metrics.Factory, upstreams []string) *Reporter {
	decisions := f.NewCounter(prometheus.CounterOpts{
		Name: "decisions_total",
		Help: "Total routing decisions, labeled by chosen upstream and outcome.",
	}, []string{"upstream", "outcome"})

	errors := f.NewCounter(prometheus.CounterOpts{
		Name: "forwarding_errors_total",
		Help: "Total router-originated forwarding failures, labeled by upstream and reason.",
	}, []string{"upstream", "reason"})

	r := &Reporter{
		decisions: decisions,
		errors:    errors,
		decHandle: make(map[decisionKey]prometheus.Counter),
		errHandle: make(map[errorKey]prometheus.Counter),
	}

	outcomes := []Outcome{OutcomeMatch, OutcomeDefault, OutcomeSystem}
	reasons := []string{reasonNoConnection, reasonStreamSetup}
	for _, u := range upstreams {
		for _, o := range outcomes {
			r.decHandle[decisionKey{u, o}] = decisions.WithLabelValues(u, o.String())
		}
		for _, reason := range reasons {
			r.errHandle[errorKey{u, reason}] = errors.WithLabelValues(u, reason)
		}
	}
	r.decHandle[decisionKey{upstreamUnknown, OutcomeUnroutable}] = decisions.WithLabelValues(
		upstreamUnknown,
		OutcomeUnroutable.String(),
	)

	return r
}

// Decision increments the decision counter for the chosen upstream and outcome.
func (r *Reporter) Decision(upstream string, outcome Outcome) {
	if c, ok := r.decHandle[decisionKey{upstream, outcome}]; ok {
		c.Inc()
		return
	}

	r.decisions.WithLabelValues(upstream, outcome.String()).Inc()
}

// ForwardingError increments the forwarding-error counter for the upstream and
// reason.
func (r *Reporter) ForwardingError(upstream, reason string) {
	if c, ok := r.errHandle[errorKey{upstream, reason}]; ok {
		c.Inc()
		return
	}

	r.errors.WithLabelValues(upstream, reason).Inc()
}
