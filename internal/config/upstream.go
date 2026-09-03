package config

import (
	"fmt"
	"strings"

	"github.com/temporalio/temporal-proxy/internal/cloud"
	"github.com/temporalio/temporal-proxy/pkg/validation"
)

type (
	// Upstream describes a single upstream Temporal cluster the proxy connects
	// workers to along with configuration for that remote cluster. Name
	// identifies the upstream so routing rules can refer to it; it must be
	// unique within the config.
	//
	// Cloud declares the upstream to be Temporal Cloud, which turns on
	// Cloud-specific namespace rules. It is only needed for an address
	// [cloud.IsEndpoint] does not recognize, such as a private-link hostname; a
	// .tmprl.cloud address is detected without it.
	Upstream struct {
		Name        string            `yaml:"name"`
		Cloud       bool              `yaml:"cloud"`
		Listen      ListenConfig      `yaml:",inline"`
		Namespaces  NamespaceConfig   `yaml:"namespaces"`
		Credentials *CredentialConfig `yaml:"credentials"`
	}

	UpstreamList []Upstream

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

// Validate checks the upstream name, dial target, and namespace configuration.
// A templated hostPort (containing a text/template action) is resolved
// per-request, so it is not checked as a literal host:port here; a static
// hostPort still is.
func (u *Upstream) Validate() error {
	return validation.Validate(
		"",
		validation.Field("name", u.Name, validation.Required[string]()),
		validation.WhenRules(
			func() bool { return !isTemplated(u.Listen.HostPort) },
			validation.Field("hostPort", u.Listen.HostPort, validation.IsHostPort()),
		),
		validation.WhenRules(
			func() bool { return u.Listen.TLS != nil },
			func() validation.Errors { return u.Listen.TLS.validateOutbound() },
		),
		validation.Nested("namespaces", &u.Namespaces),
		validation.WhenRules(
			func() bool { return u.Credentials != nil },
			validation.Nested("credentials", u.Credentials),
		),
		validation.WhenRules(
			func() bool { return u.Credentials != nil && u.Listen.TLS == nil },
			func() validation.Errors {
				return validation.Errors{{Field: "credentials", Message: "requires TLS to the upstream"}}
			},
		),
		validation.WhenRules(u.IsCloud, u.cloudRules()...),
	)
}

// IsCloud reports whether the upstream is Temporal Cloud, either because it says
// so or because its address is a Cloud endpoint. The TLS server name counts too:
// a private-link upstream reaches Cloud through a per-VPC hostname but still
// pins Cloud's certificate.
func (u *Upstream) IsCloud() bool {
	if u.Cloud || cloud.IsEndpoint(u.Listen.HostPort) {
		return true
	}

	return u.Listen.TLS != nil && cloud.IsEndpoint(u.Listen.TLS.ServerName)
}

// IsTemplated reports whether the upstream must be resolved per request because
// its hostPort, or its TLS server name when one is configured, contains a
// text/template action.
func (u *Upstream) IsTemplated() bool {
	if isTemplated(u.Listen.HostPort) {
		return true
	}

	return u.Listen.TLS != nil && isTemplated(u.Listen.TLS.ServerName)
}

// cloudRules builds the namespace rules that only hold for a Temporal Cloud
// upstream, where every remote name has to be a Cloud namespace identifier.
// They live here rather than under [NamespaceRules.Validate] because that runs a
// level down and cannot see whether the upstream is Cloud.
func (u *Upstream) cloudRules() []validation.Rule {
	nsRules := &u.Namespaces.Rules

	return []validation.Rule{
		func() validation.Errors {
			id, found := strings.CutPrefix(nsRules.Suffix, ".")
			if nsRules.Suffix == "" || (found && cloud.ValidateAccountID(id) == nil) {
				return nil
			}

			return validation.Errors{{
				Subject: "namespaces.rules",
				Field:   "suffix",
				Message: `must be ".<account-id>" for a Temporal Cloud upstream`,
			}}
		},
		func() validation.Errors {
			var errs validation.Errors
			for i, m := range nsRules.Overrides {
				// An empty remote is already reported as required.
				if m.Remote == "" || cloud.ValidateNamespace(m.Remote) == nil {
					continue
				}

				errs = append(errs, validation.Error{
					Subject: fmt.Sprintf("namespaces.rules.overrides[%d]", i),
					Field:   "remote",
					Message: "must be a Temporal Cloud namespace (<name>.<account-id>)",
				})
			}

			return errs
		},
	}
}

func (ul UpstreamList) Validate() error {
	names := make([]string, len(ul))
	hostPorts := make([]string, len(ul))
	for i, s := range ul {
		names[i] = s.Name
		hostPorts[i] = s.Listen.HostPort
	}

	return validation.Validate(
		"",
		validation.Field("[name]", names, validation.Unique[string]()),
		validation.Field("[hostPort]", hostPorts, validation.Unique[string]()),
		validation.Children("", ul, func(u *Upstream) error {
			return u.Validate()
		}),
	)
}

// Validate checks the namespace translation rules.
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

// Validate checks that override entries are complete and that no local or
// remote name is mapped more than once.
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

// Configured reports whether the rules translate anything. When false the
// prefix, suffix, and overrides are all empty and Remote and Local are identity,
// so callers can skip installing translation entirely.
func (r *NamespaceRules) Configured() bool {
	return r.Prefix != "" || r.Suffix != "" || len(r.Overrides) > 0
}

// Validate requires both the local and remote namespace names.
func (m *NamespaceMapping) Validate() error {
	return validation.Validate(
		"",
		validation.Field("local", m.Local, validation.Required[string]()),
		validation.Field("remote", m.Remote, validation.Required[string]()),
	)
}

// isTemplated reports whether s contains a text/template action ("{{ ... }}").
// Templated upstream targets (e.g. "{{ .RemoteNamespace }}.acme.cloud:7233")
// are rendered per-request, so they cannot be validated as a literal host:port
// at config-load time.
func isTemplated(s string) bool {
	return strings.Contains(s, "{{") && strings.Contains(s, "}}")
}
