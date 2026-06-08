package config

import (
	"fmt"
	"strings"

	"github.com/temporalio/temporal-proxy/pkg/validation"
)

type (
	// Upstream describes a single upstream Temporal cluster the proxy connects
	// workers to along with configuration for that remote cluster.
	Upstream struct {
		Listen     ListenConfig    `yaml:",inline"`
		Namespaces NamespaceConfig `yaml:"namespaces"`
	}

	// NamespaceConfig groups the namespace translation rules for an upstream.
	NamespaceConfig struct {
		Rules NamespaceRules `yaml:"rules"`
	}

	// NamespaceRules translates namespace names between the local view that
	// workers use and the remote names registered on the upstream cluster.
	//
	// The default translation is to wrap or unwrap a Prefix and Suffix:
	// Remote("payments") returns Prefix+"payments"+Suffix, and Local of that
	// returns "payments". When an explicit Overrides entry matches, the
	// override takes precedence over the prefix/suffix rule.
	NamespaceRules struct {
		Prefix    string             `yaml:"prefix"`
		Suffix    string             `yaml:"suffix"`
		Overrides []NamespaceMapping `yaml:"overrides"`

		localToRemote map[string]string
		remoteToLocal map[string]string
	}

	// NamespaceMapping is one explicit local/remote namespace pair, used to
	// short-circuit the prefix/suffix rule for namespaces whose names do not
	// follow the convention.
	NamespaceMapping struct {
		Local  string `yaml:"local"`
		Remote string `yaml:"remote"`
	}
)

func (u *Upstream) Validate() error {
	return validation.Validate(
		"",
		validation.Nested("", &u.Listen),
		validation.Nested("namespaces", &u.Namespaces),
	)
}

func (c *NamespaceConfig) Validate() error {
	return validation.Validate(
		"",
		validation.Nested("rules", &c.Rules),
	)
}

// Local returns the local namespace name that corresponds to remoteNS. If an
// override matches it wins; otherwise the configured Prefix and Suffix are
// stripped from remoteNS.
func (r *NamespaceRules) Local(remoteNS string) string {
	if v, ok := r.remoteToLocal[remoteNS]; ok {
		return v
	}

	return strings.TrimPrefix(strings.TrimSuffix(remoteNS, r.Suffix), r.Prefix)
}

// Remote returns the remote namespace name that corresponds to localNS. If an
// override matches it wins; otherwise localNS is wrapped with the configured
// Prefix and Suffix.
func (r *NamespaceRules) Remote(localNS string) string {
	if v, ok := r.localToRemote[localNS]; ok {
		return v
	}

	return fmt.Sprintf("%s%s%s", r.Prefix, localNS, r.Suffix)
}

func (r *NamespaceRules) UnmarshalYAML(unmarshal func(any) error) error {
	type raw NamespaceRules

	var decoded raw
	if err := unmarshal(&decoded); err != nil {
		return err
	}

	*r = NamespaceRules(decoded)
	r.localToRemote = make(map[string]string)
	r.remoteToLocal = make(map[string]string)
	for _, mapping := range r.Overrides {
		r.localToRemote[mapping.Local] = mapping.Remote
		r.remoteToLocal[mapping.Remote] = mapping.Local
	}

	return nil
}

func (r *NamespaceRules) Validate() error {
	if len(r.Overrides) == 0 {
		return nil
	}

	locals := make([]string, len(r.Overrides))
	remotes := make([]string, len(r.Overrides))
	rules := make([]validation.Rule, len(r.Overrides)+2)

	for i := range r.Overrides {
		locals[i] = r.Overrides[i].Local
		remotes[i] = r.Overrides[i].Remote
		rules[i+2] = validation.Nested(fmt.Sprintf("overrides[%d]", i), &r.Overrides[i])
	}

	rules[0] = validation.Field("overrides[local]", locals, validation.Unique[string]())
	rules[1] = validation.Field("overrides[remote]", remotes, validation.Unique[string]())
	return validation.Validate("", rules...)
}

func (m *NamespaceMapping) Validate() error {
	return validation.Validate(
		"",
		validation.Field("local", m.Local, validation.Required[string]()),
		validation.Field("remote", m.Remote, validation.Required[string]()),
	)
}
