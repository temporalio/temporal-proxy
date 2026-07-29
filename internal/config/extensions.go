package config

import (
	"errors"

	"github.com/temporalio/temporal-proxy/pkg/validation"
)

type (
	// ExtensionServer addresses an operator-run gRPC server implementing the
	// extension APIs under api/, currently api.kms.v1.EncryptionService, the
	// pluggable Key Encryption Key provider. The proxy has built-in KMS providers
	// (awskms, azurekeyvault, gcpkms); an extension server is how an operator plugs
	// in a backend the proxy does not support natively, such as an on-prem HSM or
	// an internal key service.
	//
	// Name identifies the server within the configuration so other blocks can
	// reference it, and must be unique across the list. Credentials, when set,
	// attach per-request credentials to the outbound calls and require TLS,
	// since sending them over a plaintext connection would expose them on the wire.
	//
	// Unlike Upstream, an extension server is dialed at a fixed address rather
	// than resolved per request, so a templated hostPort is rejected outright
	// instead of being deferred to request time.
	ExtensionServer struct {
		Name        string            `yaml:"name"`
		Listen      ListenConfig      `yaml:",inline"`
		Credentials *CredentialConfig `yaml:"credentials"`
	}

	// ExtensionServerList is the configured set of extension servers. It exists
	// as a named type so the checks that span the whole collection - name and
	// address uniqueness - live alongside the per-entry checks instead of in
	// the parent Config.
	ExtensionServerList []ExtensionServer
)

// Validate checks a single extension server: a name is required, hostPort must
// be a literal host:port with no template action, and any TLS block must be
// valid for dialing out. Credentials without TLS are rejected. Failures are
// unattributed, leaving the caller to stamp the path - ExtensionServerList
// supplies the index.
func (s *ExtensionServer) Validate() error {
	return validation.Validate(
		"",
		validation.Field("name", s.Name, validation.Required[string]()),
		validation.Field("hostPort", s.Listen.HostPort, isLiteralHostPort()),
		validation.WhenRules(
			func() bool { return s.Listen.TLS != nil },
			func() validation.Errors { return s.Listen.TLS.validateOutbound() },
		),
		validation.WhenRules(
			func() bool { return s.Credentials != nil },
			validation.Nested("credentials", s.Credentials),
		),
		validation.WhenRules(
			func() bool { return s.Credentials != nil && s.Listen.TLS == nil },
			func() validation.Errors {
				return validation.Errors{{Field: "credentials", Message: "requires TLS to the extension server"}}
			},
		),
	)
}

// Validate checks every entry and enforces that names and addresses are unique
// across the list: two servers sharing a name would make a reference ambiguous,
// and two sharing an address is a copy-paste error rather than a useful
// configuration. Uniqueness failures are reported on a "[name]"/"[hostPort]"
// field because they belong to the collection rather than to any one entry,
// while per-entry failures are stamped with a "[i]" subject. Both compose onto
// the parent's path, so Config surfaces them as "extensionServers[name]" and
// "extensionServers[0]". An empty or nil list is valid.
func (sl ExtensionServerList) Validate() error {
	names := make([]string, len(sl))
	hostPorts := make([]string, len(sl))
	for i, s := range sl {
		names[i] = s.Name
		hostPorts[i] = s.Listen.HostPort
	}

	return validation.Validate(
		"",
		validation.Field("[name]", names, validation.Unique[string]()),
		validation.Field("[hostPort]", hostPorts, validation.Unique[string]()),
		validation.Children("", sl, func(s *ExtensionServer) error {
			return s.Validate()
		}),
	)
}

// isLiteralHostPort accepts only a static host:port. The template check runs
// first and short-circuits, because a template is not merely unresolvable here
// but indistinguishable from a valid address once it carries a literal port:
// "{{ .Namespace }}.acme.cloud:9090" splits cleanly into host and port and
// would otherwise be accepted, then dialed verbatim.
func isLiteralHostPort() validation.Check[string] {
	hostPort := validation.IsHostPort()

	return func(s string) error {
		if isTemplated(s) {
			return errors.New("must be a literal host:port; templates are not resolved for extension servers")
		}

		return hostPort(s)
	}
}
