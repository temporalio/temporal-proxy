package config_test

import (
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
