package proxy

import (
	"context"
	"fmt"
	"net"
	"slices"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/temporalio/temporal-proxy/internal/config"
	"github.com/temporalio/temporal-proxy/internal/template"
	"github.com/temporalio/temporal-proxy/internal/transport/meta"
)

type (
	// DynamicResolver is a [connect.Resolver] that renders an upstream's dial
	// target (and optional TLS server name) per request from the local
	// namespace and request metadata. It always reports IsStatic as false, so a
	// [connect.Conn] built from it resolves lazily on every call. A non-templated
	// hostPort renders to itself, so a DynamicResolver also serves upstreams with
	// a fixed address. Construct one with NewDynamicResolver.
	DynamicResolver struct {
		name       string
		host       *template.Template[template.UpstreamContext]
		serverName *template.Template[template.UpstreamContext]
		remote     func(string) string
		opts       func(d RouteData) ([]grpc.DialOption, error)
	}

	// RouteData is passed to the options factory once a request has been
	// resolved. It carries the template context used for rendering plus the
	// resolved TLS server name, so the factory can build dial options (e.g.
	// credentials whose SNI depends on the rendered server name).
	RouteData struct {
		template.UpstreamContext
		ResolvedServerName string
	}

	// ResolverOption configures a DynamicResolver at construction.
	ResolverOption func(*DynamicResolver)
)

// NewDynamicResolver builds a DynamicResolver for up. It compiles the hostPort
// and TLS server-name templates (failing if either is malformed) and applies
// opts. By default the remote namespace equals the local one and no dial options
// are added; use WithRemoteNamespacer and WithOptionsFactory to change that.
func NewDynamicResolver(up *config.Upstream, opts ...ResolverOption) (*DynamicResolver, error) {
	host, err := template.ParseUpstream(up.Listen.HostPort)
	if err != nil {
		return nil, fmt.Errorf("failed to parse upstream host template %q: %w", up.Name, err)
	}

	tsn := ""
	if up.Listen.TLS != nil {
		tsn = up.Listen.TLS.ServerName
	}

	serverName, err := template.ParseUpstream(tsn)
	if err != nil {
		return nil, fmt.Errorf("failed to parse upstream TLS server name template %q: %w", up.Name, err)
	}

	r := &DynamicResolver{
		name:       up.Name,
		host:       host,
		serverName: serverName,
		remote:     func(s string) string { return s },
		opts:       func(RouteData) ([]grpc.DialOption, error) { return nil, nil },
	}

	for _, opt := range opts {
		opt(r)
	}

	return r, nil
}

// WithRemoteNamespacer sets the function that maps the local namespace to the
// remote one, making RemoteNamespace available to the templates.
func WithRemoteNamespacer(f func(string) string) ResolverOption {
	return func(r *DynamicResolver) { r.remote = f }
}

// WithOptionsFactory sets the function that produces the dial options for a
// resolved request. It receives the rendered host and server name via RouteData.
func WithOptionsFactory(f func(RouteData) ([]grpc.DialOption, error)) ResolverOption {
	return func(r *DynamicResolver) { r.opts = f }
}

// IsStatic reports that a DynamicResolver always resolves per request.
func (r *DynamicResolver) IsStatic() bool {
	return false
}

// Resolve renders the dial target and server name from ctx and returns the pool
// cache key, the dial target, and the dial options. The cache key combines the
// target and rendered server name so that two requests to the same address with
// different server names get distinct pooled connections. It fails with
// codes.Internal (naming the upstream and template) when a template fails to
// render, the rendered address is empty or malformed, or the options factory
// errors; nothing is dialed in those cases.
func (r *DynamicResolver) Resolve(ctx context.Context) (string, string, []grpc.DialOption, error) {
	localNS := meta.NamespaceFrom(ctx)
	data := template.UpstreamContext{
		LocalNamespace:  localNS,
		RemoteNamespace: r.remote(localNS),
		Metadata:        lastMetadataValues(ctx),
	}

	target, err := r.host.Render(data)
	if err != nil {
		return "", "", nil, status.Errorf(
			codes.Internal,
			"proxy: upstream %q failed to render hostPort %q: %v",
			r.name,
			r.host.String(),
			err,
		)
	}

	if invalidAddress(target) {
		return "", "", nil, status.Errorf(
			codes.Internal,
			"proxy: upstream %q rendered invalid hostPort %q from template %q",
			r.name,
			target,
			r.host.String(),
		)
	}

	svrName, err := r.serverName.Render(data)
	if err != nil {
		return "", "", nil, status.Errorf(
			codes.Internal,
			"proxy: upstream %q failed to render serverName %q: %v",
			r.name,
			r.serverName.String(),
			err,
		)
	}

	opts, err := r.opts(RouteData{
		UpstreamContext:    data,
		ResolvedServerName: svrName,
	})
	if err != nil {
		return "", "", nil, status.Errorf(
			codes.Internal,
			"proxy: upstream %q failed to build dial options: %v",
			r.name,
			err,
		)
	}

	return target + "\x00" + svrName, target, opts, nil
}

// lastMetadataValues flattens the request's outgoing metadata to one value per
// key, keeping the last (most recently added) value. gRPC lowercases metadata
// keys, so template lookups must use lowercase keys.
func lastMetadataValues(ctx context.Context) map[string]string {
	md, ok := metadata.FromOutgoingContext(ctx)
	if !ok {
		return nil
	}

	out := make(map[string]string, len(md))
	for k, v := range md {
		if len(v) > 0 {
			out[k] = v[len(v)-1]
		}
	}

	return out
}

// invalidAddress reports whether addr is unusable as a dial target: empty, or
// missing a host label (e.g. ":7233" or ".acme:7233" produced when a referenced
// namespace or metadata value renders empty). It strips a leading gRPC scheme
// ("dns:///", "passthrough:///") and tolerates a single trailing dot so a
// fully-qualified name ("acme.example.:7233") is accepted.
func invalidAddress(addr string) bool {
	if strings.TrimSpace(addr) == "" {
		return true
	}

	hp := addr
	if i := strings.Index(hp, "://"); i >= 0 {
		hp = hp[i+3:]
		hp = strings.TrimPrefix(hp, "/") // handle the "dns:///" triple-slash form
	}

	host, _, err := net.SplitHostPort(hp)
	if err != nil {
		host = hp // no port present; treat the whole remainder as the host
	}

	host = strings.TrimSpace(host)
	if host == "" {
		return true
	}

	host = strings.TrimSuffix(host, ".")
	return slices.Contains(strings.Split(host, "."), "")
}
