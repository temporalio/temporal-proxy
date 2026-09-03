package config

import (
	"github.com/temporalio/temporal-proxy/pkg/validation"
)

// DefaultCloudAPIHostPort is Temporal Cloud's control plane, which serves
// CloudService. It is where a translated method goes, and is the same address
// the Cloud SDK dials by default.
const DefaultCloudAPIHostPort = "saas-api.tmprl.cloud:443"

// CloudAPI describes Temporal Cloud's control plane, which answers the methods
// Cloud does not serve on a namespace frontend.
//
// Configuring it turns on method translation: a request for a method the proxy
// knows a Cloud equivalent for (currently WorkflowService.ListNamespaces) is
// captured on its way to whichever upstream routing chose, rewritten to the
// CloudService method that answers it, and sent here instead. Routing is not
// involved and needs no rule, because where CloudService lives is a fixed fact
// about Temporal Cloud rather than a policy an operator chooses. Leave the block
// out entirely and no method is translated.
//
// It is deliberately not an entry in Upstreams: nothing is routed to it, so it
// needs no listener, socket, or health entry - only a connection.
type CloudAPI struct {
	Listen      ListenConfig      `yaml:",inline"`
	Credentials *CredentialConfig `yaml:"credentials"`
}

// Upstream renders the control plane as an [Upstream], so its connection is
// dialled by the same resolver, TLS, and credential machinery as any other
// rather than by a second code path. The name is fixed and never referenced by
// routing.
func (c *CloudAPI) Upstream() *Upstream {
	up := &Upstream{
		Name:        "cloud-api",
		Listen:      c.Listen,
		Credentials: c.Credentials,
	}

	if up.Listen.HostPort == "" {
		up.Listen.HostPort = DefaultCloudAPIHostPort
	}

	return up
}

// Validate checks the dial target and credentials. hostPort is optional and
// defaults to [DefaultCloudAPIHostPort], so only a value that was supplied is
// checked. Credentials require TLS for the same reason they do on an upstream:
// sending them over a plaintext connection would expose them on the wire.
func (c *CloudAPI) Validate() error {
	return validation.Validate(
		"",
		validation.WhenRules(
			func() bool { return c.Listen.HostPort != "" },
			validation.Field("hostPort", c.Listen.HostPort, validation.IsHostPort()),
		),
		validation.WhenRules(
			func() bool { return c.Listen.TLS != nil },
			func() validation.Errors { return c.Listen.TLS.validateOutbound() },
		),
		validation.WhenRules(
			func() bool { return c.Credentials != nil },
			validation.Nested("credentials", c.Credentials),
		),
		validation.WhenRules(
			func() bool { return c.Credentials != nil && c.Listen.TLS == nil },
			func() validation.Errors {
				return validation.Errors{{Field: "credentials", Message: "requires TLS to the Cloud API"}}
			},
		),
	)
}
