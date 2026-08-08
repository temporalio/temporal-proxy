package proxy_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/temporalio/temporal-proxy/internal/config"
	"github.com/temporalio/temporal-proxy/internal/proxy"
)

func TestResolverForStatic(t *testing.T) {
	t.Parallel()

	up := &config.Upstream{
		Name:   "primary",
		Listen: config.ListenConfig{HostPort: "127.0.0.1:7233"},
	}

	res, err := proxy.ResolverFor(up, nil, nil)
	require.NoError(t, err)
	require.True(t, res.IsStatic(), "a plain hostPort resolves once and is reused")

	key, target, _, err := res.Resolve(t.Context())
	require.NoError(t, err)
	require.Equal(t, "127.0.0.1:7233", key)
	require.Equal(t, "127.0.0.1:7233", target)
}

func TestResolverForTemplated(t *testing.T) {
	t.Parallel()

	up := &config.Upstream{
		Name:   "cloud",
		Listen: config.ListenConfig{HostPort: "{{ .RemoteNamespace }}.tmprl.cloud:7233"},
	}

	res, err := proxy.ResolverFor(up, nil, nil)
	require.NoError(t, err)
	require.False(t, res.IsStatic(), "a templated hostPort resolves per request")
}

func TestResolverForRejectsUnknownTemplateField(t *testing.T) {
	t.Parallel()

	up := &config.Upstream{
		Name:   "cloud",
		Listen: config.ListenConfig{HostPort: "{{ .Nope }}.tmprl.cloud:7233"},
	}

	_, err := proxy.ResolverFor(up, nil, nil)
	require.Error(t, err)
}
