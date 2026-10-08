package proxy_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/temporalio/temporal-proxy/internal/config"
	"github.com/temporalio/temporal-proxy/internal/proxy"
)

func TestResolverForStatic(t *testing.T) {
	t.Parallel()

	up := prepared(t, config.Upstream{
		Name:   "primary",
		Listen: config.ListenConfig{HostPort: "127.0.0.1:7233"},
	})

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

	up := prepared(t, config.Upstream{
		Name:   "cloud",
		Listen: config.ListenConfig{HostPort: "{{ .RemoteNamespace }}.tmprl.cloud:7233"},
	})

	res, err := proxy.ResolverFor(up, nil, nil)
	require.NoError(t, err)
	require.False(t, res.IsStatic(), "a templated hostPort resolves per request")
}

func TestResolverForSRV(t *testing.T) {
	t.Parallel()

	const target = "srv:///_grpc._tcp.frontend.temporal.svc"
	up := prepared(t, config.Upstream{
		Name:   "frontends",
		Listen: config.ListenConfig{HostPort: target, Insecure: true},
	})

	res, err := proxy.ResolverFor(up, nil, nil)
	require.NoError(t, err)
	require.True(t, res.IsStatic())

	_, got, opts, err := res.Resolve(t.Context())
	require.NoError(t, err)
	require.Equal(t, target, got)

	// Without the SRV builder in opts, gRPC falls back to its dns resolver and
	// the canonical target becomes "dns:///srv:///...".
	cc, err := grpc.NewClient(got, opts...)
	require.NoError(t, err)
	t.Cleanup(func() { _ = cc.Close() })

	require.Equal(t, target, cc.CanonicalTarget())
}
