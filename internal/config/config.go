package config

import (
	"cmp"
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
		APITranslations  APITranslations     `yaml:"apiTranslations"`
		AllowedServices  Services            `yaml:"allowedServices"`
		Auth             *AuthConfig         `yaml:"auth"`
		Encryption       Encryption          `yaml:"encryption"`
		ExtensionServers ExtensionServerList `yaml:"extensionServers"`
		Health           Health              `yaml:"health"`
		HTTP             HTTP                `yaml:"http"`
		Metrics          Metrics             `yaml:"metrics"`
		Routing          Routing             `yaml:"routing"`
		Upstreams        UpstreamList        `yaml:"upstreams"`

		// RetiredCodecServer catches the top-level codecServer block, which moved
		// under http. Without it the old key would be silently ignored, leaving
		// the codec server off for a config that says it is on.
		RetiredCodecServer any `yaml:"codecServer"`
	}
)

// Load reads and parses the YAML config specified in the Reader, then prepares
// it (see [Config.Prepare]). Values of the form ${VAR} are replaced with the
// corresponding environment variable.
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

	if err := cfg.Prepare(); err != nil {
		return nil, err
	}

	return &cfg, nil
}

// LoadFile reads, parses, and prepares the YAML config file at path.
// Values of the form ${VAR} are replaced with the corresponding environment variable.
func LoadFile(path string) (*Config, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer func() { _ = f.Close() }()

	return Load(f)
}

// Prepare fills defaults into zero fields, validates the result, and compiles
// derived state such as namespace override maps. It recomputes on every call,
// so a config mutated after Prepare can be prepared again; on error no compiled
// state is left behind. It modifies the config in place, including slice
// elements, and is not safe for concurrent use.
func (c *Config) Prepare() error {
	c.reset()
	c.applyDefaults()

	if err := c.validate(); err != nil {
		return err
	}

	if err := c.compile(); err != nil {
		c.reset()
		return err
	}

	return nil
}

// validate requires at least one upstream, checks the listen configuration and
// every upstream, requires upstream names to be unique, and checks that every
// cross-reference names something configured: routing references an upstream,
// while encryption key URIs and external authentication reference an extension
// server. The codec server's auth, if enabled, is also checked against known
// extension servers. A missing upstream surfaces on the "upstreams" field.
// Failures are stamped with the failing node's YAML path as the subject (e.g.
// "upstreams[0].namespaces.rules.overrides[1]"). A duplicate name surfaces on the
// "upstreams[name]" field, an unknown routing reference on the
// "routing"/"routing.rules[i]" subject, and an unknown extension server on the
// referring "encryption.*", "auth.external", or "http.codecServer.auth.external"
// subject.
func (c *Config) validate() error {
	rules := []validation.Rule{
		validation.Field("upstreams", c.Upstreams, func(us UpstreamList) error {
			if len(us) == 0 {
				return errors.New("at least one upstream is required")
			}

			return nil
		}),
		validation.Nested("", &c.Listen),
		validation.Nested("", &c.AllowedServices),
		validation.WhenRules(
			func() bool { return c.RetiredCodecServer != nil },
			func() validation.Errors {
				return validation.Errors{{
					Field:   "codecServer",
					Message: "moved to http.codecServer, with hostPort, insecure, and tls set on http itself",
				}}
			},
		),
		validation.Nested("encryption", &c.Encryption),
		validation.Nested("extensionServers", &c.ExtensionServers),
		validation.Nested("health", &c.Health),
		validation.Nested("http", &c.HTTP),
		validation.Nested("metrics", &c.Metrics),
		validation.Nested("routing", &c.Routing),
		validation.WhenRules(func() bool { return c.Auth != nil }, validation.Nested("auth", c.Auth)),
		validation.Nested("apiTranslations", &c.APITranslations),
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

	rules = append(rules, c.Auth.referentialRules("auth.external", knownExtensions)...)
	rules = append(rules, c.HTTP.referentialRules(knownExtensions)...)
	rules = append(rules, c.Routing.referentialRules(known)...)
	rules = append(rules, c.Encryption.referentialRules(knownExtensions)...)
	return validation.Validate("", rules...)
}

// compile builds every node's derived state. It runs only after validate
// passes, and uses the same helpers validate does.
func (c *Config) compile() error {
	for i := range c.Upstreams {
		if err := c.Upstreams[i].compile(); err != nil {
			return fmt.Errorf("upstreams[%d]: %w", i, err)
		}
	}

	if err := c.Routing.compile(); err != nil {
		return fmt.Errorf("routing: %w", err)
	}

	return nil
}

// reset drops every node's derived state.
func (c *Config) reset() {
	for i := range c.Upstreams {
		c.Upstreams[i].reset()
	}

	c.Routing.reset()
}

// applyDefaults fills the fields that most configs omit. An absent block never
// reaches an unmarshaler, so defaults live here rather than in UnmarshalYAML,
// and a Config built in code gets them too.
func (c *Config) applyDefaults() {
	c.AllowedServices = c.AllowedServices.Allowed()
	c.Metrics.HostPort = cmp.Or(c.Metrics.HostPort, ":9090")
	c.Metrics.Namespace = cmp.Or(c.Metrics.Namespace, "tmprl_proxy")
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
