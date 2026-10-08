package config

import (
	"fmt"
	"strings"

	"github.com/temporalio/temporal-proxy/internal/cloud"
	"github.com/temporalio/temporal-proxy/internal/template"
	"github.com/temporalio/temporal-proxy/internal/transport/resolve"
	"github.com/temporalio/temporal-proxy/pkg/validation"
)

type (
	// Upstream describes a single upstream Temporal Service the proxy connects
	// workers to, along with the configuration for reaching it. Name identifies
	// the upstream so routing rules can refer to it; it must be unique within
	// the config.
	//
	// Cloud declares the upstream to be Temporal Cloud, which turns on
	// Cloud-specific namespace rules. It is only needed for an address
	// [cloud.IsEndpoint] does not recognize, such as a private-link hostname; a
	// .tmprl.cloud address, or a TLS server name that is one, is detected
	// without it.
	//
	// The proxy dials an upstream over TLS unless Listen says otherwise, so a
	// plaintext upstream must set its Insecure field.
	Upstream struct {
		Name        string            `yaml:"name"`
		Cloud       bool              `yaml:"cloud"`
		Listen      ListenConfig      `yaml:",inline"`
		Namespaces  NamespaceConfig   `yaml:"namespaces"`
		Credentials *CredentialConfig `yaml:"credentials"`
		Connection  ConnectionConfig  `yaml:"connection"`

		hostTmpl       *template.Template[template.UpstreamContext]
		serverNameTmpl *template.Template[template.UpstreamContext]
	}

	// UpstreamList is the configured set of upstreams, named so the checks that
	// span the whole collection live alongside the per-entry checks.
	UpstreamList []Upstream

	// NamespaceConfig groups the namespace translation rules for an upstream.
	NamespaceConfig struct {
		Rules NamespaceRules `yaml:"rules"`
	}

	// NamespaceRules translates namespace names between the local view that
	// workers use and the remote names registered on the upstream Temporal
	// Service.
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

// Validate checks the upstream name, dial target, outbound TLS, namespace, and
// connection configuration. Credentials require TLS, insecure conflicts with a
// tls block, and a Cloud upstream must use Cloud namespace names.
// A templated hostPort is parsed as a template and not checked as a literal
// host:port; a static hostPort still is.
func (u *Upstream) Validate() error {
	return validation.Validate(
		"",
		validation.Field("name", u.Name, validation.Required[string]()),
		func() validation.Errors {
			_, _, errs := u.parseTemplates()
			return errs
		},
		validation.WhenRules(
			u.hasLiteralHostPort,
			validation.Field("hostPort", u.Listen.HostPort, upstreamHostPort()),
		),
		validation.WhenRules(
			func() bool { return u.Listen.TLS != nil },
			func() validation.Errors { return u.Listen.TLS.validateOutbound() },
		),
		validation.Nested("namespaces", &u.Namespaces),
		validation.Nested("connection", &u.Connection),
		validation.WhenRules(
			func() bool { return u.Credentials != nil },
			validation.Nested("credentials", u.Credentials),
		),
		validation.WhenRules(
			func() bool { return u.Credentials != nil && u.Listen.Insecure },
			func() validation.Errors {
				return validation.Errors{{Field: "credentials", Message: "requires TLS to the upstream"}}
			},
		),
		u.Listen.insecureRule(),
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
// its hostPort or TLS server name contains a template action. Panics if the
// upstream was never prepared by [Config.Prepare].
func (u *Upstream) IsTemplated() bool {
	host, serverName := u.Templates()
	return !host.IsLiteral() || !serverName.IsLiteral()
}

// Templates returns the parsed hostPort and TLS server name templates; a
// missing server name is the empty literal. Panics if the upstream was never
// prepared by [Config.Prepare].
func (u *Upstream) Templates() (hostPort, serverName *template.Template[template.UpstreamContext]) {
	if u.hostTmpl == nil {
		panic("config: Upstream used before Config.Prepare")
	}

	return u.hostTmpl, u.serverNameTmpl
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

// compile builds the upstream's derived state. Validation has already passed.
func (u *Upstream) compile() error {
	host, serverName, errs := u.parseTemplates()
	if len(errs) > 0 {
		return errs
	}

	u.hostTmpl, u.serverNameTmpl = host, serverName
	u.Namespaces.Rules.compile()

	return nil
}

// reset drops the upstream's derived state.
func (u *Upstream) reset() {
	u.hostTmpl, u.serverNameTmpl = nil, nil
	u.Namespaces.Rules.reset()
}

// parseTemplates parses the hostPort and TLS server name as upstream templates,
// reporting each failure on the field it came from.
func (u *Upstream) parseTemplates() (host, serverName *template.Template[template.UpstreamContext], errs validation.Errors) {
	host, err := template.ParseUpstream(u.Listen.HostPort)
	if err != nil {
		errs = append(errs, validation.Error{Field: "hostPort", Message: err.Error()})
	}

	sn := ""
	if u.Listen.TLS != nil {
		sn = u.Listen.TLS.ServerName
	}

	serverName, err = template.ParseUpstream(sn)
	if err != nil {
		errs = append(errs, validation.Error{Subject: "tls", Field: "serverName", Message: err.Error()})
	}

	return host, serverName, errs
}

// hasLiteralHostPort reports whether the hostPort parses and has no template
// actions, so it can be checked as a literal host:port.
func (u *Upstream) hasLiteralHostPort() bool {
	host, err := template.ParseUpstream(u.Listen.HostPort)
	return err == nil && host.IsLiteral()
}

// Validate checks every upstream and requires names and hostPorts to be unique
// across the list.
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
// stripped from remoteNS. Panics if the rules were never prepared by
// [Config.Prepare].
func (r *NamespaceRules) Local(remoteNS string) string {
	r.mustBeCompiled()

	if v, ok := r.remoteToLocal[remoteNS]; ok {
		return v
	}

	return strings.TrimPrefix(strings.TrimSuffix(remoteNS, r.Suffix), r.Prefix)
}

// Remote returns the remote namespace name that corresponds to localNS. If an
// override matches it wins; otherwise localNS is wrapped with the configured
// Prefix and Suffix. Panics if the rules were never prepared by
// [Config.Prepare].
func (r *NamespaceRules) Remote(localNS string) string {
	r.mustBeCompiled()

	if v, ok := r.localToRemote[localNS]; ok {
		return v
	}

	return fmt.Sprintf("%s%s%s", r.Prefix, localNS, r.Suffix)
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

// compile builds the override lookup maps from Overrides.
func (r *NamespaceRules) compile() {
	r.localToRemote = make(map[string]string, len(r.Overrides))
	r.remoteToLocal = make(map[string]string, len(r.Overrides))
	for _, m := range r.Overrides {
		r.localToRemote[m.Local] = m.Remote
		r.remoteToLocal[m.Remote] = m.Local
	}
}

// reset drops the compiled lookup maps.
func (r *NamespaceRules) reset() {
	r.localToRemote, r.remoteToLocal = nil, nil
}

// mustBeCompiled panics unless compile has run, so an unprepared config fails
// loudly instead of silently ignoring its overrides.
func (r *NamespaceRules) mustBeCompiled() {
	if r.localToRemote == nil {
		panic("config: NamespaceRules used before Config.Prepare")
	}
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
// Extension servers use it to reject templated hostPorts, which only upstreams
// may carry.
func isTemplated(s string) bool {
	return strings.Contains(s, "{{") && strings.Contains(s, "}}")
}

// upstreamHostPort accepts a host:port, or an srv:/// target naming the SRV
// record that lists the upstream's backends.
func upstreamHostPort() validation.Check[string] {
	hostPort := validation.IsHostPort()

	return func(s string) error {
		if resolve.IsTarget(s) {
			_, err := resolve.ParseTarget(s)
			return err
		}

		return hostPort(s)
	}
}
