package config

import (
	"github.com/temporalio/temporal-proxy/internal/cloud"
	"github.com/temporalio/temporal-proxy/pkg/validation"
)

// CloudAPI overrides how the proxy reaches Temporal Cloud's control plane, which
// answers the methods Cloud does not serve on a namespace frontend.
//
// The block is optional and usually absent. An upstream that [Upstream.IsCloud]
// recognizes gets method translation on its own, over a connection to
// [cloud.APIHostPort] carrying that upstream's credentials - the same API key
// authorizes both, so there is nothing more to say. Configure this only to reach
// a different Cloud environment, or when the control plane needs credentials or
// TLS the upstream does not supply.
//
// It is deliberately not an entry in Upstreams: nothing is routed to it, so it
// needs no name, listener, socket, or health entry - only a connection.
type CloudAPI struct {
	Listen      ListenConfig      `yaml:",inline"`
	Credentials *CredentialConfig `yaml:"credentials"`
}

// Upstream renders the control plane as an [Upstream] for the Cloud upstream
// src, so its connection is dialled by the same resolver, TLS, and credential
// machinery as any other rather than by a second code path. A nil receiver is
// the unconfigured case and yields the inherited defaults, so callers need not
// branch on whether the block is present.
//
// Credentials are inherited from src because a Temporal Cloud API key authorizes
// the control plane as well as the frontend. TLS is not: the control plane is a
// different host, so src's server name or client certificate would not apply to
// it, and a default outbound TLS configuration is used instead.
//
// When this block is present its own tls is authoritative, absent included, the
// same way it is on an upstream - an absent tls dials in plaintext. That is only
// reachable for a control plane with no credentials, since Validate rejects
// credentials without TLS, so a key still cannot be sent in the clear.
//
// The name is derived from src rather than fixed, so two Cloud upstreams with
// different credentials get distinct connections instead of sharing whichever
// was dialled first.
func (c *CloudAPI) Upstream(src *Upstream) *Upstream {
	up := &Upstream{
		Name:        src.Name + "/cloud-api",
		Cloud:       true,
		Listen:      ListenConfig{HostPort: cloud.APIHostPort, TLS: &TLSConfig{}},
		Credentials: src.Credentials,
	}

	if c == nil {
		return up
	}

	if c.Listen.HostPort != "" {
		up.Listen.HostPort = c.Listen.HostPort
	}

	up.Listen.TLS = c.Listen.TLS

	if c.Credentials != nil {
		up.Credentials = c.Credentials
	}

	return up
}

// IsEndpoint reports whether the configured control plane addresses Temporal
// Cloud. It is false only when an operator pointed the block somewhere else,
// which is legitimate for a test double or a private environment, so callers
// report it rather than reject it.
func (c *CloudAPI) IsEndpoint() bool {
	if c == nil || c.Listen.HostPort == "" {
		return true
	}

	return cloud.IsEndpoint(c.Listen.HostPort)
}

// Validate checks the dial target and credentials. hostPort is optional and
// defaults to [cloud.APIHostPort], so only a value that was supplied is checked.
// Credentials require TLS for the same reason they do on an upstream: sending
// them over a plaintext connection would expose them on the wire.
//
// An address that is not a Cloud endpoint is not rejected here. Nothing but a
// Cloud deployment serves CloudService, but a test double or a private
// environment legitimately does not carry the Cloud domain, and the proxy has no
// way to tell that apart from a typo. It is reported at startup instead, which
// mirrors how a namespace Cloud would reject is handled for a templated upstream.
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
