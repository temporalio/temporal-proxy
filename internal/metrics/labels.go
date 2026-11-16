package metrics

import (
	"context"
	"maps"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"unicode/utf8"

	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc/metadata"

	"go.temporal.io/proxy/internal/config"
)

// maxLabelValueLen bounds a metadata label's value in bytes. The value is
// supplied by the caller and the registry never evicts a series, so an
// unbounded one is a memory-growth vector.
const maxLabelValueLen = 256

// MaxLabelValues bounds how many distinct values one metadata label reports.
// The value is supplied by the caller, request-scoped reporters run before
// authentication, and the registry never evicts a series, so without a bound
// any caller could add a series per request until the process runs out of
// memory. Once a label has seen this many values, every new one is reported as
// [LabelValueOther]; values already seen keep reporting as themselves.
const MaxLabelValues = 100

// LabelValueOther is the value a metadata label reports once it has seen
// [MaxLabelValues] distinct values.
const LabelValueOther = "other"

type (
	// MetadataLabels is the ordered set of inbound metadata headers reported as
	// extra labels on request-scoped collectors. The zero MetadataLabels carries
	// none, which is what a deployment that configures none runs, so a reporter
	// holding one adds no labels and reads no metadata.
	//
	// Copies share the record of values each label has seen, so the
	// [MaxLabelValues] bound holds across every reporter given the same labels.
	MetadataLabels struct {
		names   []string    // Prometheus label names, in configured order.
		headers []string    // Lowercased metadata keys, parallel to names.
		seen    []*valueSet // Values each label has reported, parallel to names.
	}

	// valueSet is the bounded set of values one label has reported. Reads load an
	// immutable map without locking, since nearly every request carries a value
	// already seen; the rare admission copies it under mu and swaps it in. Once
	// the set is full, full lets an unseen value be turned away without taking
	// mu, so a flood of new values cannot contend on it: mu is held at most
	// [MaxLabelValues] times for admissions over the set's life.
	valueSet struct {
		mu     sync.Mutex
		values atomic.Pointer[map[string]struct{}]
		full   atomic.Bool
	}
)

// NewMetadataLabels builds the ordered labels for cfg. Headers are lowercased
// here because gRPC canonicalizes metadata keys while a config preserves
// whatever case its author wrote; names keep their case, which Prometheus
// distinguishes.
func NewMetadataLabels(cfg []config.MetricLabel) MetadataLabels {
	if len(cfg) == 0 {
		return MetadataLabels{}
	}

	l := MetadataLabels{
		names:   make([]string, len(cfg)),
		headers: make([]string, len(cfg)),
		seen:    make([]*valueSet, len(cfg)),
	}

	for i, label := range cfg {
		l.names[i] = label.Name
		l.headers[i] = strings.ToLower(label.Header)
		l.seen[i] = newValueSet()
	}

	return l
}

// WithFixedLabels returns r with labels stamped onto every collector registered
// through it, so a constant an operator configures once reaches every series the
// proxy publishes rather than only the ones emitted while serving a request. It
// returns r unchanged when there are none, so a deployment configuring no fixed
// labels registers collectors with only their own labels.
func WithFixedLabels(r prometheus.Registerer, labels map[string]string) prometheus.Registerer {
	if len(labels) == 0 {
		return r
	}

	return prometheus.WrapRegistererWith(prometheus.Labels(labels), r)
}

// Len is the number of extra labels, and zero for a MetadataLabels carrying
// none.
func (l MetadataLabels) Len() int { return len(l.names) }

// Names returns the extra label names in configured order, for appending to a
// collector's own label names at construction. The result is a copy.
func (l MetadataLabels) Names() []string { return slices.Clone(l.names) }

// AppendValues appends this request's label values to dst in [MetadataLabels.Names]
// order and returns the extended slice, so a caller resolves them once and
// appends them to more than one label list. It returns dst untouched when none
// are configured, without reading ctx.
func (l MetadataLabels) AppendValues(ctx context.Context, dst []string) []string {
	if len(l.headers) == 0 {
		return dst
	}

	// A context with no incoming metadata yields a nil MD, and Get on that is a
	// nil-map read, so an absent request context needs no separate branch.
	md, _ := metadata.FromIncomingContext(ctx)
	for i, h := range l.headers {
		dst = append(dst, l.seen[i].admit(labelValue(md.Get(h))))
	}

	return dst
}

func newValueSet() *valueSet {
	s := &valueSet{}
	s.values.Store(&map[string]struct{}{})
	return s
}

// admit returns v when the label has reported it before or has room for it, and
// [LabelValueOther] once [MaxLabelValues] distinct values are taken. The empty
// value, which a request without the header reports, is always admitted and
// does not count against the bound.
func (s *valueSet) admit(v string) string {
	if v == "" {
		return v
	}

	if _, ok := (*s.values.Load())[v]; ok {
		return v
	}

	// The map is stored before full is set and never changes after, so once full
	// is seen a fresh load is the final set. Re-checking it covers a value
	// admitted as the last one between the read above and this one.
	if s.full.Load() {
		if _, ok := (*s.values.Load())[v]; ok {
			return v
		}

		return LabelValueOther
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	// Re-read under the lock: another request may have admitted v, or filled the
	// set, since the unlocked read.
	cur := *s.values.Load()
	if _, ok := cur[v]; ok {
		return v
	}

	if len(cur) >= MaxLabelValues {
		return LabelValueOther
	}

	next := maps.Clone(cur)
	next[v] = struct{}{}
	s.values.Store(&next)

	if len(next) >= MaxLabelValues {
		s.full.Store(true)
	}

	return v
}

// labelValue reduces a header's values to the one reported as a label. A header
// the request did not carry yields "", because Prometheus has no notion of an
// absent label value. So does one whose value is not valid UTF-8: client_golang
// panics on those, and a caller should not be able to provoke that. Where a
// header carries several values the last wins, as it does everywhere else the
// proxy reduces metadata to a single value.
func labelValue(vals []string) string {
	if len(vals) == 0 {
		return ""
	}

	// Validated before truncating, so a value that is already invalid is
	// cleared rather than silently shortened into something that looks fine.
	v := vals[len(vals)-1]
	if !utf8.ValidString(v) {
		return ""
	}

	if len(v) > maxLabelValueLen {
		v = v[:maxLabelValueLen]

		// Cutting on a byte boundary can split a rune, so drop the partial tail
		// rather than emit a value client_golang would reject.
		for len(v) > 0 && !utf8.ValidString(v) {
			v = v[:len(v)-1]
		}
	}

	return v
}
