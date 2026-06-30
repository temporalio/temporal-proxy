package config

import (
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/goccy/go-yaml"

	"github.com/temporalio/temporal-proxy/pkg/validation"
)

type (
	// Config is the top-level proxy configuration.
	Config struct {
		Listen    ListenConfig `yaml:",inline"`
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

// Validate checks the listen configuration and every upstream, and requires
// upstream names to be unique. Per-upstream failures are stamped with an
// "upstreams[i]" subject; a duplicate name surfaces on the "upstreams[name]"
// field.
func (c *Config) Validate() error {
	rules := []validation.Rule{
		validation.Nested("", &c.Listen),
	}

	names := make([]string, len(c.Upstreams))
	for i := range c.Upstreams {
		names[i] = c.Upstreams[i].Name
		rules = append(rules, validation.Nested(fmt.Sprintf("upstreams[%d]", i), &c.Upstreams[i]))
	}

	rules = append(rules, validation.Field("upstreams[name]", names, validation.Unique[string]()))
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
