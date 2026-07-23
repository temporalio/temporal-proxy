package config

import (
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"

	"github.com/goccy/go-yaml"

	"github.com/temporalio/temporal-proxy/pkg/validation"
)

type (
	// Config is the top-level proxy configuration.
	Config struct {
		Listen     ListenConfig `yaml:",inline"`
		Encryption Encryption   `yaml:"encryption"`
		Routing    Routing      `yaml:"routing"`
		Upstreams  []Upstream   `yaml:"upstreams"`
		Auth       *AuthConfig  `yaml:"auth"`
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
	if err := yaml.UnmarshalWithOptions([]byte(expanded), &cfg, yaml.CustomUnmarshaler(unmarshalURL)); err != nil {
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

// Validate requires at least one upstream, checks the listen configuration and
// every upstream, requires upstream names to be unique, and checks that every
// routing reference names a configured upstream. A missing upstream surfaces on
// the "upstreams" field. Failures are stamped with the failing node's YAML path
// as the subject (e.g. "upstreams[0].namespaces.rules.overrides[1]"). A
// duplicate name surfaces on the "upstreams[name]" field, and an unknown
// routing reference on the "routing"/"routing.rules[i]" subject.
func (c *Config) Validate() error {
	rules := []validation.Rule{
		validation.Field("upstreams", c.Upstreams, func(us []Upstream) error {
			if len(us) == 0 {
				return errors.New("at least one upstream is required")
			}

			return nil
		}),
		validation.Nested("", &c.Listen),
		validation.Nested("encryption", &c.Encryption),
		validation.Nested("routing", &c.Routing),
		validation.WhenRules(func() bool { return c.Auth != nil }, validation.Nested("auth", c.Auth)),
		validation.Children("upstreams", c.Upstreams, func(u *Upstream) error { return u.Validate() }),
	}

	names := make([]string, len(c.Upstreams))
	hostPorts := make([]string, len(c.Upstreams))
	known := make(map[string]struct{}, len(c.Upstreams))
	for i := range c.Upstreams {
		names[i] = c.Upstreams[i].Name
		hostPorts[i] = c.Upstreams[i].Listen.HostPort
		known[names[i]] = struct{}{}
	}

	rules = append(rules, validation.Field("upstreams[name]", names, validation.Unique[string]()))
	rules = append(rules, validation.Field("upstreams[hostPort]", hostPorts, validation.Unique[string]()))
	rules = append(rules, c.Routing.referentialRules(known)...)
	return validation.Validate("", rules...)
}

// unmarshalURL decodes a YAML scalar into a url.URL by parsing its string form.
// It is registered as a goccy CustomUnmarshaler so config fields typed url.URL
// (and []url.URL) can be written as plain YAML strings. goccy passes the raw
// node bytes (quotes and trailing newline included), so the value is decoded as
// a string before it is parsed.
func unmarshalURL(u *url.URL, b []byte) error {
	var s string
	if err := yaml.Unmarshal(b, &s); err != nil {
		return err
	}

	parsed, err := url.Parse(s)
	if err != nil {
		return fmt.Errorf("invalid url %q: %w", s, err)
	}

	*u = *parsed
	return nil
}
