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

	"go.temporal.io/proxy/internal/config"
	"go.temporal.io/proxy/internal/proxy"
	"go.temporal.io/proxy/internal/transport/meta"
	"go.temporal.io/proxy/pkg/logger"
	"go.temporal.io/proxy/pkg/logger/tag"
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
		{"scheme prefixed", `dns:///{{ "host" }}.acme:7233`, false},
		{"srv target", `srv:///_grpc._tcp.{{ "orders" }}.svc`, false},
		{"no port", `{{ "orders" }}.acme.example`, false},
		{"templated port", `127.0.0.1:{{ "7233" }}`, false},
		{"ipv6 zone", `[fe80::1%25eth0]:7233{{ "" }}`, true},
		{"port out of range", `{{ "host" }}.acme:70000`, true},
		{"empty port", `{{ "host" }}.acme:`, true},
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

func TestDynamicResolverRequiresPlainHosts(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		hostPort   string
		serverName string
		namespace  string
		wantErr    string
	}{
		{"query", "{{ .RemoteNamespace }}.tmprl.cloud:7233", "", "other.example?", "invalid hostPort"},
		{"path", "{{ .RemoteNamespace }}.tmprl.cloud:7233", "", "other.example/", "invalid hostPort"},
		{"userinfo", "{{ .RemoteNamespace }}.tmprl.cloud:7233", "", "user@other.example", "invalid hostPort"},
		{"fragment", "{{ .RemoteNamespace }}.tmprl.cloud:7233", "", "other.example#", "invalid hostPort"},
		{"port", "{{ .RemoteNamespace }}.tmprl.cloud:7233", "", "other.example:443", "invalid hostPort"},
		{"percent", "{{ .RemoteNamespace }}.tmprl.cloud:7233", "", "other%2eexample", "invalid hostPort"},
		{"space", "{{ .RemoteNamespace }}.tmprl.cloud:7233", "", "other example", "invalid hostPort"},
		{"scheme path", "dns:///{{ .RemoteNamespace }}.tmprl.cloud:7233", "", "other.example/x", "invalid hostPort"},
		{"authority", "dns://resolver.example/{{ .RemoteNamespace }}.acme.example:7233", "", "orders", "invalid hostPort"},
		{"port value", "acme.example:{{ .RemoteNamespace }}", "", "7233/other", "invalid hostPort"},
		{"scheme", "{{ .RemoteNamespace }}.acme.example:7233", "", "unix:///x", "invalid hostPort"},
		{"passthrough scheme", "{{ .RemoteNamespace }}.acme.example:7233", "", "passthrough:///x", "invalid hostPort"},
		{"scheme separator", "a{{ .RemoteNamespace }}.acme.example:7233", "", ":///b", "invalid hostPort"},
		{"second scheme", "dns:///{{ .RemoteNamespace }}.acme.example:7233", "", "unix:///x", "invalid hostPort"},
		{"server name", "acme.example:7233", "{{ .RemoteNamespace }}.sni.example", "other.example?", "invalid serverName"},
		{"server name port", "acme.example:7233", "{{ .RemoteNamespace }}.sni.example", "other.example:443", "invalid serverName"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			res := proxy.NewDynamicResolver(prepared(t, upstreamWith("u", tt.hostPort, tt.serverName)))

			_, _, _, err := res.Resolve(meta.WithNamespace(t.Context(), tt.namespace))
			require.Error(t, err)
			require.Equal(t, codes.Internal, status.Code(err))
			require.ErrorContains(t, err, tt.wantErr)
		})
	}
}

func TestDynamicResolverRequiresPlainHostsFromMetadata(t *testing.T) {
	t.Parallel()

	res := proxy.NewDynamicResolver(prepared(t, upstreamWith("m", `{{ index .Metadata "dc" }}.acme:7233`, "")))
	ctx := metadata.NewOutgoingContext(t.Context(), metadata.Pairs("dc", "other.example?"))

	_, _, _, err := res.Resolve(ctx)
	require.Error(t, err)
	require.Equal(t, codes.Internal, status.Code(err))
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
