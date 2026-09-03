package metrics

import (
	"context"
	"slices"
	"strings"
	"unicode/utf8"

	"google.golang.org/grpc/metadata"

	"github.com/temporalio/temporal-proxy/internal/config"
)

// maxTagValueLen bounds a tag's label value in bytes. The value is supplied by
// the caller and the registry never evicts a series, so an unbounded one is a
// memory-growth vector.
const maxTagValueLen = 256

// Tags is the ordered set of inbound metadata headers reported as extra labels
// on request-scoped collectors. The zero Tags carries none, which is what a
// deployment that configures no tags runs, so a reporter holding one keeps its
// label set and its emit path exactly as they were.
type Tags struct {
	labels  []string // Prometheus label names, in configured order.
	headers []string // Lowercased metadata keys, parallel to labels.
}

// NewTags builds the ordered Tags for cfg. Headers are lowercased here because
// gRPC canonicalizes metadata keys while a config preserves whatever case its
// author wrote; labels keep their case, which Prometheus distinguishes.
func NewTags(cfg []config.MetricTag) Tags {
	if len(cfg) == 0 {
		return Tags{}
	}

	t := Tags{
		labels:  make([]string, len(cfg)),
		headers: make([]string, len(cfg)),
	}

	for i, tag := range cfg {
		t.labels[i] = tag.Label
		t.headers[i] = strings.ToLower(tag.Header)
	}

	return t
}

// Len is the number of extra labels, and zero for a Tags carrying none.
func (t Tags) Len() int { return len(t.labels) }

// Labels returns the extra label names in configured order, for appending to a
// collector's own label names at construction. The result is a copy.
func (t Tags) Labels() []string { return slices.Clone(t.labels) }

// AppendValues appends this request's tag values to dst in [Tags.Labels] order
// and returns the extended slice, so a caller resolves them once and appends
// them to more than one label list. It returns dst untouched when no tags are
// configured, without reading ctx.
func (t Tags) AppendValues(ctx context.Context, dst []string) []string {
	if len(t.headers) == 0 {
		return dst
	}

	// A context with no incoming metadata yields a nil MD, and Get on that is a
	// nil-map read, so an absent request context needs no separate branch.
	md, _ := metadata.FromIncomingContext(ctx)
	for _, h := range t.headers {
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

	if len(v) > maxTagValueLen {
		v = v[:maxTagValueLen]

		// Cutting on a byte boundary can split a rune, so drop the partial tail
		// rather than emit a value client_golang would reject.
		for len(v) > 0 && !utf8.ValidString(v) {
			v = v[:len(v)-1]
		}
	}

	return v
}
