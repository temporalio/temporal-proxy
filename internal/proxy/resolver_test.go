package proxy_test

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/temporalio/temporal-proxy/internal/config"
	"github.com/temporalio/temporal-proxy/internal/proxy"
	"github.com/temporalio/temporal-proxy/internal/transport/meta"
	"github.com/temporalio/temporal-proxy/pkg/logger"
	"github.com/temporalio/temporal-proxy/pkg/logger/tag"
)

func TestDynamicResolverIsStatic(t *testing.T) {
	t.Parallel()

	res := proxy.NewDynamicResolver(prepared(t, upstreamWith("u", "host:7233", "")))
	require.False(t, res.IsStatic())
}

func TestDynamicResolverResolve(t *testing.T) {
	t.Parallel()

	up := upstreamWith("cloud", "{{ .RemoteNamespace }}.acme.example:7233", "{{ .RemoteNamespace }}.sni.example")

	var got proxy.RouteData
	res := proxy.NewDynamicResolver(
		prepared(t, up),
		proxy.WithRemoteNamespacer(func(s string) string { return s + "-remote" }),
		proxy.WithOptionsFactory(func(d proxy.RouteData) ([]grpc.DialOption, error) {
			got = d
			return []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}, nil
		}),
	)

	key, target, opts, err := res.Resolve(meta.WithNamespace(t.Context(), "orders"))
	require.NoError(t, err)

	require.Equal(t, "orders-remote.acme.example:7233", target)
	require.Equal(t, "orders-remote.acme.example:7233\x00orders-remote.sni.example", key,
		"cache key combines target and rendered server name")
	require.Len(t, opts, 1)

	// The options factory sees the rendered server name and namespaces.
	require.Equal(t, "orders-remote.sni.example", got.ResolvedServerName)
	require.Equal(t, "orders", got.LocalNamespace)
	require.Equal(t, "orders-remote", got.RemoteNamespace)
}

func TestDynamicResolverUsesLastMetadataValue(t *testing.T) {
	t.Parallel()

	res := proxy.NewDynamicResolver(prepared(t, upstreamWith("m", `{{ index .Metadata "dc" }}.acme:7233`, "")))

	md := metadata.MD{}
	md.Append("dc", "old", "new")
	ctx := metadata.NewOutgoingContext(t.Context(), md)

	_, target, _, err := res.Resolve(ctx)
	require.NoError(t, err)
	require.Equal(t, "new.acme:7233", target, "the last metadata value wins")
}

func TestDynamicResolverFailsLoudOnOptionsError(t *testing.T) {
	t.Parallel()

	res := proxy.NewDynamicResolver(
		prepared(t, upstreamWith("u", "host:7233", "")),
		proxy.WithOptionsFactory(func(proxy.RouteData) ([]grpc.DialOption, error) {
			return nil, errors.New("boom")
		}),
	)

	_, _, _, err := res.Resolve(meta.WithNamespace(t.Context(), "orders"))
	require.Error(t, err)
	require.Equal(t, codes.Internal, status.Code(err))
	require.ErrorContains(t, err, "failed to build dial options")
}

func TestDynamicResolverRejectsInvalidAddresses(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		host    string
		wantErr bool
	}{
		{"empty metadata leaves an empty label", `{{ index .Metadata "dc" }}.acme:7233`, true},
		{"leading dot", ".acme:7233", true},
		{"interior double dot", "acme..example:7233", true},
		{"missing host", ":7233", true},
		{"normal host", "orders.acme.example:7233", false},
		{"trailing-dot fqdn", "acme.example.:7233", false},
		{"ipv6 literal", "[::1]:7233", false},
		{"scheme prefixed", `dns:///{{ "host" }}:7233`, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			res := proxy.NewDynamicResolver(prepared(t, upstreamWith("u", tt.host, "")))

			_, _, _, err := res.Resolve(meta.WithNamespace(t.Context(), "orders"))
			if tt.wantErr {
				require.Error(t, err)
				require.Equal(t, codes.Internal, status.Code(err))
				return
			}

			require.NoError(t, err)
		})
	}
}

func TestDynamicResolverLogsDebugEntry(t *testing.T) {
	t.Parallel()

	up := upstreamWith("cloud", "{{ .RemoteNamespace }}.acme.example:7233", "{{ .RemoteNamespace }}.sni.example")
	log := logger.NewTestLogger()
	res := proxy.NewDynamicResolver(
		prepared(t, up),
		proxy.WithRemoteNamespacer(func(s string) string { return s + "-remote" }),
		proxy.WithResolverLogger(log),
	)

	_, _, _, err := res.Resolve(meta.WithNamespace(t.Context(), "orders"))
	require.NoError(t, err)

	require.True(t, log.ContainsEntry(
		logger.LevelDebug,
		"resolved upstream target",
		tag.String("upstream", "cloud"),
		tag.String("localNamespace", "orders"),
		tag.String("remoteNamespace", "orders-remote"),
		tag.String("target", "orders-remote.acme.example:7233"),
		tag.String("serverName", "orders-remote.sni.example"),
	))
}

func TestDynamicResolverNoLoggerNoPanic(t *testing.T) {
	t.Parallel()

	res := proxy.NewDynamicResolver(prepared(t, upstreamWith("u", "host:7233", "")))
	_, _, _, err := res.Resolve(meta.WithNamespace(t.Context(), "orders"))
	require.NoError(t, err)
}

func upstreamWith(name, hostPort, serverName string) config.Upstream {
	up := config.Upstream{
		Name:   name,
		Listen: config.ListenConfig{HostPort: hostPort},
	}

	if serverName != "" {
		up.Listen.TLS = &config.TLSConfig{ServerName: serverName}
	}

	return up
}

// prepared returns u after preparing it inside a minimal config.
func prepared(t *testing.T, u config.Upstream) *config.Upstream {
	t.Helper()

	cfg := &config.Config{Listen: config.ListenConfig{HostPort: ":8080"}, Upstreams: []config.Upstream{u}}
	require.NoError(t, cfg.Prepare())

	return &cfg.Upstreams[0]
}
