package proxy

import (
	"context"
	"fmt"
	"net"
	"net/netip"
	"slices"
	"strconv"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"go.temporal.io/proxy/internal/config"
	"go.temporal.io/proxy/internal/template"
	"go.temporal.io/proxy/internal/transport/connect"
	"go.temporal.io/proxy/internal/transport/creds"
	"go.temporal.io/proxy/internal/transport/meta"
	"go.temporal.io/proxy/internal/transport/resolve"
	"go.temporal.io/proxy/pkg/logger"
	"go.temporal.io/proxy/pkg/logger/tag"
	"go.temporal.io/proxy/pkg/validation"
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
		scheme     string
		serverName *template.Template[template.UpstreamContext]
		remote     func(string) string
		opts       func(d RouteData) ([]grpc.DialOption, error)
		logger     logger.Logger
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

// NewDynamicResolver builds a DynamicResolver for the prepared upstream up,
// using its parsed hostPort and TLS server-name templates, and applies opts. By
// default the remote namespace equals the local one and no dial options are
// added; use WithRemoteNamespacer and WithOptionsFactory to change that.
func NewDynamicResolver(up *config.Upstream, opts ...ResolverOption) *DynamicResolver {
	host, serverName := up.Templates()

	r := &DynamicResolver{
		name:       up.Name,
		host:       host,
		scheme:     literalScheme(host),
		serverName: serverName,
		remote:     func(s string) string { return s },
		opts:       func(RouteData) ([]grpc.DialOption, error) { return nil, nil },
	}

	for _, opt := range opts {
		opt(r)
	}

	return r
}

// WithRemoteNamespacer sets the function that maps the local namespace to the
// remote one, making RemoteNamespace available to the templates.
func WithRemoteNamespacer(f func(string) string) ResolverOption {
	return func(r *DynamicResolver) { r.remote = f }
}

// WithOptionsFactory sets the function that produces the dial options for a
// resolved request. It receives the template context and the rendered server
// name via RouteData.
func WithOptionsFactory(f func(RouteData) ([]grpc.DialOption, error)) ResolverOption {
	return func(r *DynamicResolver) { r.opts = f }
}

// WithResolverLogger sets the logger used to emit a per-request debug entry
// after a successful resolve. Unset: no entry is emitted.
func WithResolverLogger(l logger.Logger) ResolverOption {
	return func(r *DynamicResolver) { r.logger = l }
}

// ResolverFor builds the [connect.Resolver] for an upstream. When neither the
// hostPort nor the TLS server name is templated it returns a static resolver,
// whose connection is constructed while the graph is built, opened on start,
// and reused for every request; otherwise it returns a DynamicResolver that
// renders the target and server name, and rebuilds credentials, per request.
// opts holds the request-independent dial options (namespace translation and
// outbound credentials); the upstream's connection settings and the srv:///
// resolver are appended here. log, when non-nil, is threaded into the
// DynamicResolver for per-request debug entries. credOpts are passed to the
// upstream's dialer, which is how a caller registers its TLS material with a
// [creds.Reloader].
func ResolverFor(
	upstream *config.Upstream,
	opts []grpc.DialOption,
	log logger.Logger,
	credOpts ...creds.Option,
) (connect.Resolver, error) {
	// One Dialer per upstream owns the TLS-mode decision and parses its
	// certificate material once, so a templated upstream reuses it across every
	// per-request dial (only the rendered server name varies).
	dialer := upstream.Listen.Dialer(credOpts...)
	opts = append(slices.Clone(opts), ConnectionDialOptions(&upstream.Connection)...)
	opts = append(opts, grpc.WithResolvers(resolve.NewSRVBuilder()))

	if upstream.IsTemplated() {
		translator := func(s string) string { return s }
		if upstream.Namespaces.Rules.Configured() {
			translator = upstream.Namespaces.Rules.Remote
		}

		resolverOpts := []ResolverOption{
			WithRemoteNamespacer(translator),
			WithOptionsFactory(func(data RouteData) ([]grpc.DialOption, error) {
				cred, err := dialer.DialOption(data.ResolvedServerName)
				if err != nil {
					return nil, err
				}

				return append(slices.Clone(opts), cred), nil
			}),
		}
		if log != nil {
			resolverOpts = append(resolverOpts, WithResolverLogger(log.With(tag.Component("resolver"))))
		}

		return NewDynamicResolver(upstream, resolverOpts...), nil
	}

	serverName := ""
	if upstream.Listen.TLS != nil {
		serverName = upstream.Listen.TLS.ServerName
	}

	cred, err := dialer.DialOption(serverName)
	if err != nil {
		return nil, fmt.Errorf("failed to build credentials for upstream %q: %w", upstream.Name, err)
	}

	return connect.StaticResolver(upstream.Listen.HostPort, append(slices.Clone(opts), cred)...), nil
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
// render, the rendered address or server name is not a plain host, or the
// options factory errors; nothing is dialed in those cases.
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

	if invalidAddress(target, r.scheme) {
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

	if svrName != "" && validation.IsHostname()(svrName) != nil {
		return "", "", nil, status.Errorf(
			codes.Internal,
			"proxy: upstream %q rendered invalid serverName %q from template %q",
			r.name,
			svrName,
			r.serverName.String(),
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

	if r.logger != nil {
		r.logger.Debug(
			"resolved upstream target",
			tag.String("upstream", r.name),
			tag.String("localNamespace", localNS),
			tag.String("remoteNamespace", data.RemoteNamespace),
			tag.String("target", target),
			tag.String("serverName", svrName),
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

// invalidAddress reports whether addr is not a plain dial target: the scheme
// separated by "://" must be exactly scheme (none when scheme is empty), with
// an empty authority ("dns:///host"), followed by a hostname or IP literal and
// an optional port. A single trailing dot is allowed so a fully-qualified name
// ("acme.example.:7233") is accepted.
func invalidAddress(addr, scheme string) bool {
	hp := addr
	if i := strings.Index(hp, "://"); i >= 0 {
		var ok bool
		if hp, ok = strings.CutPrefix(hp[i+3:], "/"); !ok || addr[:i] != scheme {
			return true
		}
	} else if scheme != "" {
		return true
	}

	host, port, err := net.SplitHostPort(hp)
	if err != nil {
		host = hp // no port present; treat the remainder as the host
	} else if p, err := strconv.Atoi(port); err != nil || p < 1 || p > 65535 || port[0] == '+' {
		return true
	}

	if addr, err := netip.ParseAddr(host); err == nil {
		return addr.Zone() != ""
	}

	return validation.IsHostname()(host) != nil
}

// literalScheme returns the scheme written in the template's leading literal
// text ("dns" for "dns:///{{ ... }}"), or "" when the template does not start
// with one. Only this scheme is accepted in a rendered target.
func literalScheme(t *template.Template[template.UpstreamContext]) string {
	parts := t.Parts()
	if len(parts) == 0 || parts[0].Action {
		return ""
	}

	scheme, _, ok := strings.Cut(parts[0].Text, "://")
	if !ok {
		return ""
	}

	return scheme
}
