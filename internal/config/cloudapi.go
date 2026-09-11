package config

import (
	"github.com/temporalio/temporal-proxy/internal/cloud"
	"github.com/temporalio/temporal-proxy/pkg/validation"
)

// APITranslations configures rewriting a method an upstream does not serve into
// the one that does. It is optional and usually absent, and it carries no switch:
// whether a method is translated is derived from the rest of the configuration
// rather than declared.
//
// What derives it is [Routing.NamespacelessUpstream]. The methods Temporal Cloud
// does not serve on a namespace endpoint are the ones carrying no namespace, so
// they land on the upstream serving namespace-less requests, and that upstream
// being Cloud is both necessary and sufficient for a translation to be reachable.
// An operator who wants the untranslated failure back routes those requests at a
// Temporal Service that serves them, which is the same statement made where it
// belongs.
//
// This block exists for the two things detection cannot know: which Cloud
// environment the control plane lives in, and the API key an mTLS upstream has
// none of to inherit.
//
// The zero value is the block an operator did not write, which is what almost
// every configuration has, and it answers for the default - so nothing here is a
// pointer and no caller has to check before asking. The same holds for [CloudAPI].
type APITranslations struct {
	CloudAPI CloudAPI `yaml:"cloudApi"`
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
// it, and the dial default stands instead - verification against the system root
// pool, which is what the real control plane presents.
//
// When this block is present its tls and insecure are authoritative, the same way
// they are on an upstream: an absent tls still verifies against the system roots,
// and plaintext has to be asked for. That is only reachable for a control plane
// with no credentials, since Validate rejects credentials on an insecure hop, so
// a key still cannot be sent in the clear.
//
// The name is derived from src rather than fixed, so two Cloud upstreams with
// different credentials get distinct connections instead of sharing whichever
// was dialled first.
func (c CloudAPI) Upstream(src *Upstream) *Upstream {
	up := &Upstream{
		Name:        src.Name + "/cloud-api",
		Cloud:       true,
		Listen:      ListenConfig{HostPort: cloud.APIHostPort},
		Credentials: src.Credentials,
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
func (c CloudAPI) Validate() error {
	return c.Upstream(&Upstream{Name: "cloudApi"}).Validate()
}

// Validate checks the Cloud API override as it will be dialled. An override
// nobody wrote is the zero one, which describes the defaults and passes.
func (t APITranslations) Validate() error {
	return validation.Validate("", validation.Nested("cloudApi", t.CloudAPI))
}

// IsZero reports whether the override says nothing at all, which is what an
// absent block leaves behind. Callers use it to tell a configuration that asked
// for something from one that never mentioned it.
func (c CloudAPI) IsZero() bool {
	return c == CloudAPI{}
}
