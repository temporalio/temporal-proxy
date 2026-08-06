package router

import (
	"slices"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/temporalio/temporal-proxy/internal/metrics"
)

const (
	// upstreamUnknown is the upstream label used for unroutable decisions, where
	// no upstream was chosen.
	upstreamUnknown = "unknown"

	// serviceOther is the rejection label for a service name the proxy does not
	// know. Callers choose the method they send, so labelling by the raw name
	// would let one inflate metric cardinality at will.
	serviceOther = "other"

	reasonNoConnection = "no_connection"
	reasonStreamSetup  = "stream_setup"
)

type (
	// Reporter records router telemetry to Prometheus: routing decisions, the
	// forwarding failures the router itself originates, and admission
	// rejections. It pre-resolves a counter for every meaningful (upstream,
	// outcome), (upstream, reason), and service combination so the emit path is
	// a lock-free map read; an unexpected label combination falls back to
	// CounterVec.WithLabelValues. A Reporter is safe for concurrent use.
	Reporter struct {
		decisions  *prometheus.CounterVec
		errors     *prometheus.CounterVec
		rejections *prometheus.CounterVec
		decHandle  map[decisionKey]prometheus.Counter
		errHandle  map[errorKey]prometheus.Counter
		rejHandle  map[string]prometheus.Counter
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
// upstream name list; known is the set of services the proxy can forward,
// used to bound the rejection counter's label cardinality.
func NewReporter(f *metrics.Factory, upstreams, known []string) *Reporter {
	decisions := f.NewCounter(prometheus.CounterOpts{
		Name: "decisions_total",
		Help: "Total routing decisions, labeled by chosen upstream and outcome.",
	}, []string{"upstream", "outcome"})

	errors := f.NewCounter(prometheus.CounterOpts{
		Name: "forwarding_errors_total",
		Help: "Total router-originated forwarding failures, labeled by upstream and reason.",
	}, []string{"upstream", "reason"})

	rejections := f.NewCounter(prometheus.CounterOpts{
		Name: "service_rejections_total",
		Help: "Total requests refused because the proxy does not expose the service.",
	}, []string{"service"})

	r := &Reporter{
		decisions:  decisions,
		errors:     errors,
		rejections: rejections,
		decHandle:  make(map[decisionKey]prometheus.Counter),
		errHandle:  make(map[errorKey]prometheus.Counter),
		rejHandle:  make(map[string]prometheus.Counter, len(known)+1),
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

	for _, name := range append(slices.Clone(known), serviceOther) {
		r.rejHandle[name] = rejections.WithLabelValues(name)
	}

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

// ServiceRejected increments the rejection counter for service, collapsing a
// name the proxy does not know to a single "other" series.
func (r *Reporter) ServiceRejected(service string) {
	if c, ok := r.rejHandle[service]; ok {
		c.Inc()
		return
	}

	r.rejHandle[serviceOther].Inc()
}
