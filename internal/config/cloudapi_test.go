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

	a := config.CloudAPI{}.Upstream(&config.Upstream{Name: "alpha"})
	b := config.CloudAPI{}.Upstream(&config.Upstream{Name: "beta"})

	require.Equal(t, a.Listen.HostPort, b.Listen.HostPort, "both reach the same control plane")
	require.NotEqual(t, a.Name, b.Name, "but must not share a pooled connection")
}

// TestCloudAPIUpstreamDefaults covers the unconfigured case, which is the one
// almost every deployment takes.
func TestCloudAPIUpstreamDefaults(t *testing.T) {
	t.Parallel()

	var unset config.CloudAPI
	src := &config.Upstream{Name: "frontend"}

	api := unset.Upstream(src)
	require.Equal(t, cloud.APIHostPort, api.Listen.HostPort)
	require.False(t, api.Listen.Insecure, "the real control plane is always TLS")
	require.True(t, api.IsCloud())
}

// TestAPITranslationsZeroValueIsTheUnconfiguredCase pins what the block an
// operator did not write answers. Every deployment that says nothing about
// apiTranslations reaches these on the zero value, so it has to describe the
// defaults rather than need a check in front of it.
func TestAPITranslationsZeroValueIsTheUnconfiguredCase(t *testing.T) {
	t.Parallel()

	var absent config.APITranslations

	require.True(t, absent.CloudAPI.IsZero(), "an absent block configures no control plane of its own")
	require.NoError(t, absent.Validate())
	require.True(t, absent.CloudAPI.IsSaasAPI(), "and the derived control plane is Cloud's own")
	require.Equal(t, cloud.APIHostPort, absent.CloudAPI.Upstream(&config.Upstream{Name: "frontend"}).Listen.HostPort)
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
    credentials:
      static:
        apiKey: sekrit
`

	cfg, err := config.Load(strings.NewReader(yaml))
	require.NoError(t, err)
	require.NoError(t, cfg.Validate())

	require.Empty(t, cfg.Routing.Rules, "translation needs no routing rule")
	require.True(t, cfg.APITranslations.CloudAPI.IsZero(), "and no cloudApi block")
	require.True(t, cfg.Upstreams[0].IsCloud())

	// The control plane is derived from the upstream: its own address, and the
	// upstream's credentials, since one API key authorizes both.
	api := cfg.APITranslations.CloudAPI.Upstream(&cfg.Upstreams[0])
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
    credentials:
      static:
        apiKey: upstream-key
apiTranslations:
  cloudApi:
    hostPort: saas-api.staging.tmprl.cloud:443
    credentials:
      static:
        apiKey: control-plane-key
`

	cfg, err := config.Load(strings.NewReader(yaml))
	require.NoError(t, err)
	require.NoError(t, cfg.Validate())

	api := cfg.APITranslations.CloudAPI.Upstream(&cfg.Upstreams[0])
	require.Equal(t, "saas-api.staging.tmprl.cloud:443", api.Listen.HostPort)
	require.NotEqual(t, cfg.Upstreams[0].Credentials, api.Credentials, "the override wins over inheritance")
	require.True(t, cfg.APITranslations.CloudAPI.IsSaasAPI())
}

func TestLoad_CloudAPIRejectsCredentialsOnAnInsecureHop(t *testing.T) {
	t.Parallel()

	// An absent tls block is TLS, so plaintext is what has to be asked for - and
	// asking for it with a key configured is what the rule refuses.
	const yaml = `
hostPort: 127.0.0.1:7233
routing:
  default: frontend
upstreams:
  - name: frontend
    hostPort: ns.acct.tmprl.cloud:7233
apiTranslations:
  cloudApi:
    insecure: true
    credentials:
      static:
        apiKey: sekrit
`

	cfg, err := config.Load(strings.NewReader(yaml))
	require.NoError(t, err)
	require.ErrorContains(t, cfg.Validate(), "requires TLS")
}
