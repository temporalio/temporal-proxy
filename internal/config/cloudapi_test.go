package config_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/temporalio/temporal-proxy/internal/cloud"
	"github.com/temporalio/temporal-proxy/internal/config"
)

// TestCloudAPIUpstreamsAreDistinctPerSource pins the property the pooled
// connection depends on. Every Cloud upstream's control plane resolves to the
// same address, and a pooled connection is keyed by this name; if two upstreams
// produced the same one they would share a connection, and the second would
// silently send the first's credentials.
func TestCloudAPIUpstreamsAreDistinctPerSource(t *testing.T) {
	t.Parallel()

	a := (&config.CloudAPI{}).Upstream(&config.Upstream{Name: "alpha"})
	b := (&config.CloudAPI{}).Upstream(&config.Upstream{Name: "beta"})

	require.Equal(t, a.Listen.HostPort, b.Listen.HostPort, "both reach the same control plane")
	require.NotEqual(t, a.Name, b.Name, "but must not share a pooled connection")
}

// TestCloudAPIUpstreamDefaults covers the unconfigured case, which is the one
// almost every deployment takes.
func TestCloudAPIUpstreamDefaults(t *testing.T) {
	t.Parallel()

	var unset *config.CloudAPI
	src := &config.Upstream{Name: "frontend"}

	api := unset.Upstream(src)
	require.Equal(t, cloud.APIHostPort, api.Listen.HostPort)
	require.NotNil(t, api.Listen.TLS, "the real control plane is always TLS")
	require.True(t, api.IsCloud())
}

func TestLoad_CloudUpstreamNeedsNoTranslationConfig(t *testing.T) {
	t.Parallel()

	// No routing rule and no cloudApi block. A .tmprl.cloud address is recognized
	// as Cloud on its own, which is all translation needs.
	const yaml = `
hostPort: 127.0.0.1:7233
routing:
  default: frontend
  system: frontend
upstreams:
  - name: frontend
    hostPort: ns.acct.tmprl.cloud:7233
    tls: {}
    credentials:
      static:
        apiKey: sekrit
`

	cfg, err := config.Load(strings.NewReader(yaml))
	require.NoError(t, err)
	require.NoError(t, cfg.Validate())

	require.Empty(t, cfg.Routing.Rules, "translation needs no routing rule")
	require.Nil(t, cfg.APITranslations, "and no cloudApi block")
	require.True(t, cfg.Upstreams[0].IsCloud())

	// The control plane is derived from the upstream: its own address, and the
	// upstream's credentials, since one API key authorizes both.
	api := cfg.APITranslations.Cloud().Upstream(&cfg.Upstreams[0])
	require.Equal(t, cloud.APIHostPort, api.Listen.HostPort)
	require.Equal(t, cfg.Upstreams[0].Credentials, api.Credentials)
	require.NotEqual(t, cfg.Upstreams[0].Name, api.Name, "distinct name keeps the pooled connections apart")
}

func TestLoad_CloudAPIOverridesTheDerivedControlPlane(t *testing.T) {
	t.Parallel()

	const yaml = `
hostPort: 127.0.0.1:7233
routing:
  default: frontend
upstreams:
  - name: frontend
    hostPort: ns.acct.tmprl.cloud:7233
    tls: {}
    credentials:
      static:
        apiKey: upstream-key
apiTranslations:
  cloudApi:
    hostPort: saas-api.staging.tmprl.cloud:443
    tls: {}
    credentials:
      static:
        apiKey: control-plane-key
`

	cfg, err := config.Load(strings.NewReader(yaml))
	require.NoError(t, err)
	require.NoError(t, cfg.Validate())

	api := cfg.APITranslations.Cloud().Upstream(&cfg.Upstreams[0])
	require.Equal(t, "saas-api.staging.tmprl.cloud:443", api.Listen.HostPort)
	require.NotEqual(t, cfg.Upstreams[0].Credentials, api.Credentials, "the override wins over inheritance")
	require.True(t, cfg.APITranslations.Cloud().IsEndpoint())
}

func TestLoad_CloudAPIRejectsCredentialsWithoutTLS(t *testing.T) {
	t.Parallel()

	const yaml = `
hostPort: 127.0.0.1:7233
routing:
  default: frontend
upstreams:
  - name: frontend
    hostPort: ns.acct.tmprl.cloud:7233
apiTranslations:
  cloudApi:
    credentials:
      static:
        apiKey: sekrit
`

	cfg, err := config.Load(strings.NewReader(yaml))
	require.NoError(t, err)
	require.ErrorContains(t, cfg.Validate(), "requires TLS")
}

func TestAPITranslationsIsEnabled(t *testing.T) {
	t.Parallel()

	// Default-on: translation makes a method work that cannot work otherwise, so
	// an operator opts out of a fix rather than into one.
	var absent *config.APITranslations
	require.True(t, absent.IsEnabled(), "an absent block translates")
	require.True(t, (&config.APITranslations{}).IsEnabled(), "so does one that says nothing about it")

	on, off := true, false
	require.True(t, (&config.APITranslations{Enabled: &on}).IsEnabled())
	require.False(t, (&config.APITranslations{Enabled: &off}).IsEnabled(), "only an explicit false opts out")
}

func TestLoad_APITranslationsCanBeDisabled(t *testing.T) {
	t.Parallel()

	// A Cloud upstream that would otherwise be translated, opting out.
	const yaml = `
hostPort: 127.0.0.1:7233
routing:
  default: frontend
upstreams:
  - name: frontend
    hostPort: ns.acct.tmprl.cloud:7233
    tls: {}
apiTranslations:
  enabled: false
`

	cfg, err := config.Load(strings.NewReader(yaml))
	require.NoError(t, err)
	require.NoError(t, cfg.Validate())

	require.True(t, cfg.Upstreams[0].IsCloud(), "still detected as Cloud")
	require.False(t, cfg.APITranslations.IsEnabled(), "but opted out of translation")
}
