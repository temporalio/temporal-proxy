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
		Listen           ListenConfig        `yaml:",inline"`
		AllowedServices  Services            `yaml:"allowedServices"`
		Encryption       Encryption          `yaml:"encryption"`
		ExtensionServers ExtensionServerList `yaml:"extensionServers"`
		Routing          Routing             `yaml:"routing"`
		Upstreams        UpstreamList        `yaml:"upstreams"`
		Auth             *AuthConfig         `yaml:"auth"`
		CloudAPI         *CloudAPI           `yaml:"cloudApi"`
	}
)

// Load reads and parses the YAML config specified in the Reader.
// Values of the form ${VAR} are replaced with the corresponding environment
// variable. A config that names no allowed services gets the default set.
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

	// The allowlist defaults here rather than in a Services unmarshaler because
	// an absent key never reaches one, and an absent allowedServices is how most
	// configs are written.
	cfg.AllowedServices = cfg.AllowedServices.Allowed()

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
// cross-reference names something configured: routing references an upstream,
// while encryption key URIs and external authentication reference an extension
// server. A missing upstream surfaces on the "upstreams" field. Failures are
// stamped with the failing node's YAML path as the subject (e.g.
// "upstreams[0].namespaces.rules.overrides[1]"). A duplicate name surfaces on the
// "upstreams[name]" field, an unknown routing reference on the
// "routing"/"routing.rules[i]" subject, and an unknown extension server on the
// referring "encryption.*" or "auth.external" subject.
func (c *Config) Validate() error {
	rules := []validation.Rule{
		validation.Field("upstreams", c.Upstreams, func(us UpstreamList) error {
			if len(us) == 0 {
				return errors.New("at least one upstream is required")
			}

			return nil
		}),
		validation.Nested("", &c.Listen),
		validation.Nested("", &c.AllowedServices),
		validation.Nested("encryption", &c.Encryption),
		validation.Nested("extensionServers", &c.ExtensionServers),
		validation.Nested("routing", &c.Routing),
		validation.WhenRules(func() bool { return c.Auth != nil }, validation.Nested("auth", c.Auth)),
		validation.WhenRules(func() bool { return c.CloudAPI != nil }, validation.Nested("cloudApi", c.CloudAPI)),
		validation.Nested("upstreams", &c.Upstreams),
	}

	known := make(map[string]struct{}, len(c.Upstreams))
	for i := range c.Upstreams {
		known[c.Upstreams[i].Name] = struct{}{}
	}

	knownExtensions := make(map[string]struct{}, len(c.ExtensionServers))
	for i := range c.ExtensionServers {
		knownExtensions[c.ExtensionServers[i].Name] = struct{}{}
	}

	rules = append(rules, c.Auth.referentialRules(knownExtensions)...)
	rules = append(rules, c.Routing.referentialRules(known)...)
	rules = append(rules, c.Encryption.referentialRules(knownExtensions)...)
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
