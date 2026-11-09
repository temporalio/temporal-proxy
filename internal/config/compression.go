package config

import (
	"cmp"
	"errors"
	"fmt"
	"maps"
	"slices"

	"go.temporal.io/proxy/pkg/validation"
)

const (
	// The compression.*.algorithm values. They match the encoding names the
	// compressors in pkg/compress report.
	CompressionLZ4    CompressionAlgorithm = "lz4"
	CompressionSnappy CompressionAlgorithm = "snappy"
	CompressionZstd   CompressionAlgorithm = "zstd"

	// defaultCompressionLevel is zstd's own default level.
	defaultCompressionLevel = 3

	// defaultCompressionMinBytes is where every algorithm starts saving a third
	// or more on typical JSON. Below it, much of the work is thrown away on
	// payloads that don't come out smaller.
	defaultCompressionMinBytes = 1024
)

type (
	// Compression configures compression of outbound payloads. Enabled turns it
	// on for every namespace, and Default supplies the settings for any namespace
	// without an override; it is required when Enabled is set. Overrides maps a
	// pre-translation (local) namespace name to a policy merged field by field
	// over Default, so whatever it leaves unset is inherited. Decompression does
	// not depend on any of this: payloads compressed earlier are always restored.
	Compression struct {
		Enabled   bool                         `yaml:"enabled"`
		Default   *CompressionPolicy           `yaml:"default"`
		Overrides map[string]CompressionPolicy `yaml:"overrides"`

		settings *compressionSettings
	}

	// compressionSettings is every namespace's resolved compression settings,
	// built by [Config.Prepare].
	compressionSettings struct {
		def       CompressionSettings
		overrides map[string]CompressionSettings
	}

	// CompressionPolicy is one set of compression settings. Algorithm compresses
	// new payloads. Level is the zstd effort, 1-22, and is ignored by the other
	// algorithms. Payloads whose data is smaller than MinBytes are left
	// uncompressed. Enabled is only meaningful in an override, where it supersedes
	// [Compression.Enabled] for that namespace.
	CompressionPolicy struct {
		Enabled   *bool                `yaml:"enabled"`
		Algorithm CompressionAlgorithm `yaml:"algorithm"`
		Level     *int                 `yaml:"level"`
		MinBytes  *int                 `yaml:"minBytes"`
	}

	// CompressionAlgorithm names a compression algorithm: lz4, snappy, or zstd.
	CompressionAlgorithm string

	// CompressionSettings is a namespace's compression policy with every field
	// resolved. Algorithm is empty only when Enabled is false and no policy names
	// one.
	CompressionSettings struct {
		Enabled   bool
		Algorithm CompressionAlgorithm
		Level     int
		MinBytes  int
	}
)

// Settings returns the compression settings for the local namespace ns: its
// override merged over Default, or Default alone when it has none. Panics if
// the config was never prepared by [Config.Prepare].
func (c *Compression) Settings(ns string) CompressionSettings {
	if c.settings == nil {
		panic("config: Compression used before Config.Prepare")
	}

	if s, ok := c.settings.overrides[ns]; ok {
		return s
	}

	return c.settings.def
}

// Validate requires a Default policy with an algorithm whenever compression is
// Enabled, and checks the Default policy whenever one is present. Every
// Overrides entry needs a namespace and a valid policy, and a namespace an
// override leaves enabled needs an algorithm from the override or Default.
func (c *Compression) Validate() error {
	rules := []validation.Rule{
		validation.WhenRules(
			func() bool { return c.Enabled },
			validation.Field("default", c.Default, validation.Required[*CompressionPolicy]()),
		),
		validation.WhenRules(
			func() bool { return c.Enabled && c.Default != nil && c.Default.Algorithm == "" },
			func() validation.Errors {
				return validation.Errors{{
					Subject: "default",
					Field:   "algorithm",
					Message: "required when compression is enabled",
				}}
			},
		),
		validation.WhenRules(
			func() bool { return c.Default != nil && c.Default.Enabled != nil },
			func() validation.Errors {
				return validation.Errors{{
					Subject: "default",
					Field:   "enabled",
					Message: "not allowed here; set compression.enabled instead",
				}}
			},
		),
		validation.WhenNested(func() bool { return c.Default != nil }, "default", c.Default),
	}

	// Sort the namespace keys so error ordering is deterministic across runs.
	for _, ns := range slices.Sorted(maps.Keys(c.Overrides)) {
		policy := c.Overrides[ns]
		subject := fmt.Sprintf("overrides[%s]", ns)
		rules = append(rules,
			validation.Field(subject, ns, validation.Required[string]()),
			validation.Nested(subject, &policy),
			validation.WhenRules(
				func() bool {
					s := c.resolve(&policy)
					return s.Enabled && s.Algorithm == ""
				},
				func() validation.Errors {
					return validation.Errors{{
						Subject: subject,
						Field:   "algorithm",
						Message: "required when compression is enabled; set it here or in default",
					}}
				},
			),
		)
	}

	return validation.Validate("", rules...)
}

// compile resolves the settings of every namespace. Validation has already
// passed.
func (c *Compression) compile() {
	cs := &compressionSettings{
		def:       c.resolve(nil),
		overrides: make(map[string]CompressionSettings, len(c.Overrides)),
	}

	for ns, p := range c.Overrides {
		cs.overrides[ns] = c.resolve(&p)
	}

	c.settings = cs
}

// reset drops the resolved settings.
func (c *Compression) reset() {
	c.settings = nil
}

// resolve merges override, which may be nil, over Default and the top-level
// switch. Only an override can supersede that switch; Validate rejects an
// enabled field in Default. Each field takes the first value that is set, so a
// nil pointer falls through to the next one.
func (c *Compression) resolve(override *CompressionPolicy) CompressionSettings {
	d, o := cmp.Or(c.Default, &CompressionPolicy{}), cmp.Or(override, &CompressionPolicy{})

	return CompressionSettings{
		Enabled:   *cmp.Or(o.Enabled, new(c.Enabled)),
		Algorithm: cmp.Or(o.Algorithm, d.Algorithm),
		Level:     *cmp.Or(o.Level, d.Level, new(defaultCompressionLevel)),
		MinBytes:  *cmp.Or(o.MinBytes, d.MinBytes, new(defaultCompressionMinBytes)),
	}
}

// Validate checks the fields that are set: a known algorithm, a level of 1-22,
// and a non-negative minBytes.
func (p *CompressionPolicy) Validate() error {
	return validation.Validate(
		"",
		validation.Field("algorithm", p.Algorithm, func(a CompressionAlgorithm) error {
			switch a {
			case "", CompressionLZ4, CompressionSnappy, CompressionZstd:
				return nil
			}

			return fmt.Errorf("unknown algorithm %q, expected one of lz4, snappy, zstd", string(a))
		}),
		validation.Field("level", p.Level, func(l *int) error {
			if l != nil && (*l < 1 || *l > 22) {
				return fmt.Errorf("must be 1-22, got %d", *l)
			}

			return nil
		}),
		validation.Field("minBytes", p.MinBytes, func(n *int) error {
			if n != nil && *n < 0 {
				return errors.New("must not be negative")
			}

			return nil
		}),
	)
}
