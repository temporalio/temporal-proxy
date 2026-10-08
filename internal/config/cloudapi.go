package config

import (
	"github.com/temporalio/temporal-proxy/internal/cloud"
	"github.com/temporalio/temporal-proxy/pkg/validation"
)

// APITranslations configures rewriting a method an upstream does not serve into
// the one that does. It is optional and carries no switch: every upstream
// [Upstream.IsCloud] recognizes gets translation, applied only where routing
// sends the method, so a namespace-less method is translated only when the
// upstream serving namespace-less requests is a Cloud one. The block holds what
// detection cannot know: which Cloud environment the control plane lives in,
// and an API key for an mTLS upstream. The zero value is the absent block and
// yields the defaults; the same holds for [CloudAPI].
type APITranslations struct {
	CloudAPI CloudAPI `yaml:"cloudApi"`
}

// CloudAPI overrides how the proxy reaches Temporal Cloud's control plane, which
// answers the methods Cloud does not serve on a namespace frontend. It is
// optional: an upstream [Upstream.IsCloud] recognizes gets method translation
// over a connection to [cloud.APIHostPort] carrying that upstream's
// credentials.
//
// It is required when that upstream authenticates with a client certificate:
// the Cloud Ops API accepts only an API key, so such an upstream must name one
// here or its translated methods are refused. Otherwise configure it only to
// reach a different Cloud environment. See https://docs.temporal.io/ops.
//
// It is not an entry in upstreams: the control plane is only ever a client
// connection, never a forwarder or a routing destination.
type CloudAPI struct {
	Listen      ListenConfig      `yaml:",inline"`
	Credentials *CredentialConfig `yaml:"credentials"`
}

// Upstream renders the control plane as an [Upstream] for the Cloud upstream
// src, dialled by the same resolver, TLS, and credential machinery as any
// other. The zero value yields the inherited defaults, so callers need not
// branch on whether the block is present.
//
// Credentials and connection settings are inherited from src, though the
// dataplane keeps this to a single connection whatever maxConnections says.
// TLS is not inherited: the block's tls and insecure are authoritative, an
// absent tls verifies against the system roots, and Validate rejects
// credentials on an insecure hop. The name is derived from src, so Cloud
// upstreams with different credentials get distinct connections.
func (c CloudAPI) Upstream(src *Upstream) (*Upstream, error) {
	up := c.upstream(src)
	if err := up.compile(); err != nil {
		return nil, err
	}

	return up, nil
}

// IsSaasAPI reports whether the configured control plane addresses Temporal
// Cloud's own API rather than somewhere else. It is false only when an operator
// pointed the block elsewhere, which is legitimate for a test double or a
// private environment, so callers report it rather than reject it.
func (c CloudAPI) IsSaasAPI() bool {
	if c.Listen.HostPort == "" {
		return true
	}

	return cloud.IsEndpoint(c.Listen.HostPort)
}

// Validate checks the control plane as it will actually be dialled, defaulted
// address included, by validating the [Upstream] it renders to. An address
// that is not a Cloud endpoint is not rejected, since a test double or private
// environment may legitimately use one; it is logged as a warning at startup
// instead.
func (c CloudAPI) Validate() error {
	return c.upstream(&Upstream{Name: "cloudApi"}).Validate()
}

// IsZero reports whether the override says nothing at all, which is what an
// absent block leaves behind. Callers use it to tell a configuration that asked
// for something from one that never mentioned it.
func (c CloudAPI) IsZero() bool {
	return c == CloudAPI{}
}

// upstream builds the control plane's [Upstream] for src without compiling it.
func (c CloudAPI) upstream(src *Upstream) *Upstream {
	up := &Upstream{
		Name:        src.Name + "/cloud-api",
		Cloud:       true,
		Listen:      ListenConfig{HostPort: cloud.APIHostPort},
		Credentials: src.Credentials,
		Connection:  src.Connection,
	}

	if c.Listen.HostPort != "" {
		up.Listen.HostPort = c.Listen.HostPort
	}

	up.Listen.TLS = c.Listen.TLS
	up.Listen.Insecure = c.Listen.Insecure

	if c.Credentials != nil {
		up.Credentials = c.Credentials
	}

	return up
}

// Validate checks the Cloud API override as it will be dialled. An override
// nobody wrote is the zero one, which describes the defaults and passes.
func (t APITranslations) Validate() error {
	return validation.Validate("", validation.Nested("cloudApi", t.CloudAPI))
}
