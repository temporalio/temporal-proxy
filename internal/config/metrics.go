package config

import (
	"errors"
	"fmt"
	"maps"
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
	// collector: a Prometheus namespace, unrelated to a Temporal namespace. Labels
	// controls how the published series are labeled. Load defaults HostPort and
	// Namespace, so neither is empty in a loaded config.
	Metrics struct {
		HostPort  string       `yaml:"hostPort"`
		Namespace string       `yaml:"namespace"`
		Labels    MetricLabels `yaml:"labels"`
	}

	// MetricLabels controls the labels the published series carry. Namespace
	// decides whether series that can name a Temporal Namespace report it, Fixed
	// maps a label name to a constant value stamped on every series, and Metadata
	// names the request metadata carried onto the request-scoped series.
	MetricLabels struct {
		Namespace bool              `yaml:"namespace"`
		Fixed     map[string]string `yaml:"fixed"`
		Metadata  []MetricLabel     `yaml:"metadata"`
	}

	// MetricLabel pairs an inbound request metadata Header with the Prometheus
	// label Name it is reported under. It is written in YAML as
	// "<header>:<name>".
	//
	// Choose the Header with care. Its value is published on the /metrics
	// endpoint, which is served unauthenticated, so naming a header that carries
	// a credential exposes that credential to anything able to reach the port.
	// The label also multiplies every request-scoped series rather than adding to
	// them, so a header the caller varies freely multiplies cardinality with it.
	MetricLabel struct {
		Header string
		Name   string
	}
)

// ParseMetricLabel parses the "<header>:<name>" form, trimming whitespace around
// each half. It splits on the first colon, so a name containing one survives
// parsing and is rejected by Validate instead.
func ParseMetricLabel(v string) (MetricLabel, error) {
	label := MetricLabel{}
	hdr, name, ok := strings.Cut(v, ":")
	if !ok {
		return label, errors.New("invalid metric label: format: <header>:<name>")
	}

	label.Header = strings.TrimSpace(hdr)
	label.Name = strings.TrimSpace(name)
	return label, nil
}

// Validate requires a valid host:port and a non-empty namespace, and checks the
// labels. Load defaults the first two, so a namespace failure is only reachable
// for a Metrics built directly, and a hostPort failure only for a config that
// sets one that will not parse.
func (m *Metrics) Validate() error {
	return validation.Validate(
		"",
		validation.Field("hostPort", m.HostPort, validation.IsHostPort()),
		validation.Field("namespace", m.Namespace, validation.Required[string]()),
		validation.Nested("labels", &m.Labels),
	)
}

// Validate checks every label. Two metadata labels may not share a name, and a
// fixed label may not take a name a metadata label already reports under, both
// of which Prometheus rejects as a duplicate label name.
func (l *MetricLabels) Validate() error {
	names := make([]string, len(l.Metadata))
	for i, m := range l.Metadata {
		names[i] = m.Name
	}

	rules := []validation.Rule{
		validation.Field("metadata[name]", names, validation.Unique[string]()),
		validation.Children("metadata", l.Metadata, func(m *MetricLabel) error { return m.Validate() }),
	}

	// Sorted so a config with more than one bad fixed label reports them in a
	// stable order rather than however the map happened to range.
	for _, name := range slices.Sorted(maps.Keys(l.Fixed)) {
		subject := fmt.Sprintf("fixed[%s]", name)
		rules = append(
			rules,
			validation.Field(subject, name, match(promLabelRegex), unreservedLabel(), unusedByMetadata(l.Metadata)),
			validation.Field(subject, l.Fixed[name], nonEmptyValue()),
		)
	}

	return validation.Validate("", rules...)
}

// UnmarshalYAML decodes the scalar "<header>:<name>" form, so a metadata label
// is written as a plain YAML string rather than a mapping.
func (l *MetricLabel) UnmarshalYAML(unmarshal func(any) error) error {
	var decoded string
	if err := unmarshal(&decoded); err != nil {
		return err
	}

	label, err := ParseMetricLabel(decoded)
	if err != nil {
		return err
	}

	*l = label
	return nil
}

// Validate requires a header that is a legal, unreserved, non-binary gRPC
// metadata key and a name that is a legal, unreserved Prometheus label name.
func (l *MetricLabel) Validate() error {
	return validation.Validate(
		"",
		validation.Field(
			"header",
			l.Header,
			validation.Required[string](),
			match(mdKeyRegex),
			unreservedHeader(),
			textualHeader(),
		),
		validation.Field(
			"name",
			l.Name,
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

// unusedByMetadata rejects a fixed label name a metadata label already reports
// under. Prometheus refuses a collector whose constant labels collide with its
// variable ones, so the two sets have to be disjoint.
func unusedByMetadata(metadata []MetricLabel) validation.Check[string] {
	return func(s string) error {
		for _, m := range metadata {
			if m.Name == s {
				return errors.New("must not also name a metadata label")
			}
		}

		return nil
	}
}

// nonEmptyValue rejects a fixed label carrying no value. Prometheus reads a
// blank label value as the label not being there, so an empty one asks for
// nothing while reading like it asked for something.
func nonEmptyValue() validation.Check[string] {
	return func(s string) error {
		if s == "" {
			return errors.New("must have a value")
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
