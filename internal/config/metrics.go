package config

import (
	"errors"
	"fmt"
	"regexp"
	"slices"
	"strings"

	"github.com/temporalio/temporal-proxy/pkg/validation"
)

const (
	binaryHeaderSuffix   = "-bin"  // gRPC binary metadata marker
	reservedHeaderPrefix = "grpc-" // gRPC reserved metadata prefix
	reservedLabelPrefix  = "__"    // Prometheus reserved label prefix
)

var (
	// promReservedLabels are the label names Prometheus keeps for a histogram's
	// bucket bound and a summary's quantile.
	promReservedLabels = []string{"le", "quantile"}

	// mdKeyRegex matches the characters gRPC allows in a metadata key.
	mdKeyRegex = regexp.MustCompile(`^[a-zA-Z0-9._-]+$`)

	// promLabelRegex matches the Prometheus label name grammar.
	promLabelRegex = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`)
)

type (
	// Metrics configures the Prometheus endpoint. HostPort is the address the
	// /metrics handler listens on, and Namespace is the prefix stamped onto every
	// collector: a Prometheus namespace, unrelated to a Temporal namespace. Tags
	// name the request metadata to carry through as labels. Load defaults
	// HostPort and Namespace, so neither is empty in a loaded config.
	Metrics struct {
		HostPort  string      `yaml:"hostPort"`
		Namespace string      `yaml:"namespace"`
		Tags      []MetricTag `yaml:"tags"`
	}

	// MetricTag pairs an inbound request metadata Header with the Prometheus
	// Label it is reported under. It is written in YAML as "<header>:<label>".
	//
	// Choose the Header with care. Its value is published on the /metrics
	// endpoint, which is served unauthenticated, so naming a header that carries
	// a credential exposes that credential to anything able to reach the port.
	// A tag also multiplies every request-scoped series rather than adding to
	// them, so a header the caller varies freely multiplies cardinality with it.
	MetricTag struct {
		Header string
		Label  string
	}
)

// ParseMetricTag parses the "<header>:<label>" form, trimming whitespace around
// each half. It splits on the first colon, so a label containing one survives
// parsing and is rejected by Validate instead.
func ParseMetricTag(v string) (MetricTag, error) {
	tag := MetricTag{}
	hdr, lbl, ok := strings.Cut(v, ":")
	if !ok {
		return tag, errors.New("invalid metric tag: format: <header>:<label>")
	}

	tag.Header = strings.TrimSpace(hdr)
	tag.Label = strings.TrimSpace(lbl)
	return tag, nil
}

// Validate requires a valid host:port and a non-empty namespace, and checks
// every tag. Load defaults the first two, so a namespace failure is only
// reachable for a Metrics built directly, and a hostPort failure only for a
// config that sets one that will not parse. Two tags may not share a label,
// which Prometheus rejects as a duplicate label name.
func (m *Metrics) Validate() error {
	labels := make([]string, len(m.Tags))
	for i, t := range m.Tags {
		labels[i] = t.Label
	}

	return validation.Validate(
		"",
		validation.Field("hostPort", m.HostPort, validation.IsHostPort()),
		validation.Field("namespace", m.Namespace, validation.Required[string]()),
		validation.Field("tags[label]", labels, validation.Unique[string]()),
		validation.Children("tags", m.Tags, func(t *MetricTag) error { return t.Validate() }),
	)
}

// UnmarshalYAML decodes the scalar "<header>:<label>" form, so a tag is written
// as a plain YAML string rather than a mapping.
func (t *MetricTag) UnmarshalYAML(unmarshal func(any) error) error {
	var decoded string
	if err := unmarshal(&decoded); err != nil {
		return err
	}

	tag, err := ParseMetricTag(decoded)
	if err != nil {
		return err
	}

	*t = tag
	return nil
}

// Validate requires a header that is a legal, unreserved, non-binary gRPC
// metadata key and a label that is a legal, unreserved Prometheus label name.
func (t *MetricTag) Validate() error {
	return validation.Validate(
		"",
		validation.Field(
			"header",
			t.Header,
			validation.Required[string](),
			match(mdKeyRegex),
			unreservedHeader(),
			textualHeader(),
		),
		validation.Field(
			"label",
			t.Label,
			validation.Required[string](),
			match(promLabelRegex),
			unreservedLabel(),
		),
	)
}

// match rejects a value that does not match r. An empty value yields nothing so
// the Required check on the same field owns that case and reports it once.
func match(r *regexp.Regexp) validation.Check[string] {
	return func(s string) error {
		if s == "" || r.MatchString(s) {
			return nil
		}

		return fmt.Errorf("is not valid, must match: %q", r.String())
	}
}

// textualHeader rejects a metadata key ending in "-bin". gRPC uses that suffix
// to mark a value as binary, and binary is not valid UTF-8, so such a value
// cannot be reported as a Prometheus label. The comparison is case-insensitive
// because metadata keys are.
func textualHeader() validation.Check[string] {
	return func(s string) error {
		if strings.HasSuffix(strings.ToLower(s), binaryHeaderSuffix) {
			return fmt.Errorf("must not end with %q, which gRPC uses to mark binary metadata", binaryHeaderSuffix)
		}

		return nil
	}
}

// unreservedHeader rejects a metadata key beginning with "grpc-", which gRPC
// keeps for itself. The comparison is case-insensitive because metadata keys
// are, so "GRPC-Status" is rejected alongside "grpc-status".
func unreservedHeader() validation.Check[string] {
	return func(s string) error {
		if strings.HasPrefix(strings.ToLower(s), reservedHeaderPrefix) {
			return fmt.Errorf("must not begin with %q, which gRPC reserves", reservedHeaderPrefix)
		}

		return nil
	}
}

// unreservedLabel rejects a label name Prometheus keeps for itself: one
// beginning with "__", which it refuses to register, and "le" or "quantile",
// which name a histogram's bucket bound and a summary's quantile. The reserved
// names are refused here because a histogram panics on "le" only when it first
// instantiates a series, which happens while serving a request rather than at
// construction, so nothing downstream would catch it.
func unreservedLabel() validation.Check[string] {
	return func(s string) error {
		if strings.HasPrefix(s, reservedLabelPrefix) {
			return fmt.Errorf("must not begin with %q, which Prometheus reserves", reservedLabelPrefix)
		}

		if slices.Contains(promReservedLabels, s) {
			return fmt.Errorf("must not be %q, which Prometheus reserves", s)
		}

		return nil
	}
}
