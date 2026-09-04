package config

import (
	"github.com/temporalio/temporal-proxy/internal/cloud"
	"github.com/temporalio/temporal-proxy/pkg/validation"
)

// APITranslations governs rewriting a method an upstream does not serve into the
// one that does. It is optional and usually absent: an upstream that
// [Upstream.IsCloud] recognizes has the methods Temporal Cloud does not serve on
// a frontend translated automatically, over a connection derived from it.
//
// Enabled turns that off. It defaults to on, so an absent block and an absent
// enabled both translate; only "enabled: false" does not. Nothing else can
// disable it, because whether an upstream needs translation is detected rather
// than configured - this is the operator's override of that detection, for a
// Cloud upstream whose untranslated failure is preferred to a translated answer.
type APITranslations struct {
	Enabled  *bool     `yaml:"enabled"`
	CloudAPI *CloudAPI `yaml:"cloudApi"`
}

// CloudAPI overrides how the proxy reaches Temporal Cloud's control plane, which
// answers the methods Cloud does not serve on a namespace frontend.
//
// The block is optional and usually absent. An upstream that [Upstream.IsCloud]
// recognizes gets method translation on its own, over a connection to
// [cloud.APIHostPort] carrying that upstream's credentials - the same API key
// authorizes both, so there is nothing more to say.
//
// It is required in one case. The Cloud Ops API accepts an API key only; unlike a
// namespace frontend it does not accept mTLS. An upstream authenticating with a
// client certificate therefore has no credential to inherit, and must name an API
// key here or its translated methods are refused. Beyond that, configure this
// only to reach a different Cloud environment.
//
// See https://docs.temporal.io/ops.
//
// It is deliberately not an entry in Upstreams: an upstream is a server - a
// socket, a proxy.Server, and a routing destination - and the control plane is
// only ever a client connection. Declaring it there would give it three things
// it cannot use and one it should not have: routability.
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

// Validate checks the control plane as it will actually be dialled, by
// validating the [Upstream] it renders to. That covers the same ground as any
// upstream - dial target, outbound TLS, credentials, and credentials requiring
// TLS - without restating the rules, and checks the effective configuration
// (the defaulted address included) rather than only the fields an operator
// supplied.
//
// An address that is not a Cloud endpoint is not rejected. Nothing but a Cloud
// deployment serves CloudService, but a test double or a private environment
// legitimately does not carry the Cloud domain, and the proxy has no way to tell
// that apart from a typo. It is reported at startup instead, which mirrors how a
// namespace Cloud would reject is handled for a templated upstream.
func (c *CloudAPI) Validate() error {
	return c.Upstream(&Upstream{Name: "cloudApi"}).Validate()
}

// IsEnabled reports whether method translation should be installed. An absent
// block, or a block that does not mention enabled, translates; only an explicit
// "enabled: false" does not. Default-on is deliberate - translation makes a
// method work that cannot work otherwise, so an operator opts out of a fix
// rather than into one.
func (t *APITranslations) IsEnabled() bool {
	return t == nil || t.Enabled == nil || *t.Enabled
}

// Cloud returns the Cloud API override, or nil when none is configured, so
// callers need not branch on whether the enclosing block is present.
func (t *APITranslations) Cloud() *CloudAPI {
	if t == nil {
		return nil
	}

	return t.CloudAPI
}

// Validate checks the Cloud API override when one is configured. Enabled needs no
// checking: any value is meaningful, and its absence is the default.
func (t *APITranslations) Validate() error {
	if t.CloudAPI == nil {
		return nil
	}

	return validation.Validate("", validation.Nested("cloudApi", t.CloudAPI))
}
