package config

import (
	"errors"
	"io"
	"os"

	"github.com/goccy/go-yaml"

	"github.com/temporalio/temporal-proxy/pkg/validation"
)

type (
	// Config is the top-level proxy configuration.
	Config struct {
		Listen    ListenConfig `yaml:",inline"`
		Routing   Routing      `yaml:"routing"`
		Upstreams []Upstream   `yaml:"upstreams"`
	}
)

// Load reads and parses the YAML config specified in the Reader.
// Values of the form ${VAR} are replaced with the corresponding environment variable.
func Load(r io.Reader) (*Config, error) {
	data, err := io.ReadAll(r)
	if err != nil {
		return nil, err
	}

	expanded := os.Expand(string(data), os.Getenv)

	var cfg Config
	if err := yaml.Unmarshal([]byte(expanded), &cfg); err != nil {
		return nil, err
	}

	return &cfg, nil
}

// LoadFile reads and parses the YAML config file at path.
// Values of the form ${VAR} are replaced with the corresponding environment variable.
func LoadFile(path string) (*Config, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer func() { _ = f.Close() }()

	return Load(f)
}

// Validate checks the listen configuration and every upstream, requires
// upstream names to be unique, and checks that every routing reference names a
// configured upstream. Failures are stamped with the failing node's YAML path
// as the subject (e.g. "upstreams[0].namespaces.rules.overrides[1]"). A
// duplicate name surfaces on the "upstreams[name]" field, and an unknown
// routing reference on the "routing"/"routing.rules[i]" subject.
func (c *Config) Validate() error {
	rules := []validation.Rule{
		validation.Nested("", &c.Listen),
		validation.Nested("routing", &c.Routing),
		validation.Children("upstreams", c.Upstreams, func(u *Upstream) error { return u.Validate() }),
	}

	names := make([]string, len(c.Upstreams))
	known := make(map[string]struct{}, len(c.Upstreams))
	for i := range c.Upstreams {
		names[i] = c.Upstreams[i].Name
		known[names[i]] = struct{}{}
	}

	rules = append(rules, validation.Field("upstreams[name]", names, validation.Unique[string]()))
	rules = append(rules, c.Routing.referentialRules(known)...)
	return validation.Validate("", rules...)
}

// PrimaryUpstream returns the first configured upstream. It is a temporary
// bridge for the single-upstream wiring in the proxy and router modules until
// per-request routing is in place.
func (c *Config) PrimaryUpstream() (*Upstream, error) {
	if len(c.Upstreams) == 0 {
		return nil, errors.New("no upstreams configured")
	}

	return &c.Upstreams[0], nil
}
