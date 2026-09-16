package metrics

import (
	"context"
	"slices"
	"strings"
	"unicode/utf8"

	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc/metadata"

	"github.com/temporalio/temporal-proxy/internal/config"
)

// maxLabelValueLen bounds a metadata label's value in bytes. The value is
// supplied by the caller and the registry never evicts a series, so an
// unbounded one is a memory-growth vector.
const maxLabelValueLen = 256

// MetadataLabels is the ordered set of inbound metadata headers reported as
// extra labels on request-scoped collectors. The zero MetadataLabels carries
// none, which is what a deployment that configures none runs, so a reporter
// holding one keeps its label set and its emit path exactly as they were.
type MetadataLabels struct {
	names   []string // Prometheus label names, in configured order.
	headers []string // Lowercased metadata keys, parallel to names.
}

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
	}

	for i, label := range cfg {
		l.names[i] = label.Name
		l.headers[i] = strings.ToLower(label.Header)
	}

	return l
}

// WithFixedLabels returns r with labels stamped onto every collector registered
// through it, so a constant an operator configures once reaches every series the
// proxy publishes rather than only the ones emitted while serving a request. It
// returns r unchanged when there are none, so a deployment configuring no fixed
// labels registers exactly what it did before.
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
	for _, h := range l.headers {
		dst = append(dst, labelValue(md.Get(h)))
	}

	return dst
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
