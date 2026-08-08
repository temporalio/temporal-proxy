package proxy

import (
	"context"
	"fmt"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"

	"github.com/temporalio/temporal-proxy/internal/protoutil"
	"github.com/temporalio/temporal-proxy/internal/rpc"
	"github.com/temporalio/temporal-proxy/internal/services"
)

// transportHeaders are the inbound hop's own headers, which describe that hop
// rather than the request. gRPC sets its own on the outbound call and rejects a
// pseudo-header supplied as metadata, so they are dropped rather than forwarded.
var transportHeaders = []string{"user-agent", ":authority", "content-type"}

type (
	// Forwarder forwards any allowlisted method to a single upstream, typing each
	// request and response from the proto registry rather than being generated per
	// service. The typing is load-bearing: namespace translation and payload
	// encryption are client interceptors on cc that operate on proto messages, so
	// an opaque byte passthrough (as the router uses) would silently skip both.
	// Resolved methods are cached, and a Forwarder is safe for concurrent use.
	Forwarder struct {
		cc      grpc.ClientConnInterface
		allowed services.Allowlist
		methods sync.Map // fullName -> *methodInfo
		types   protoutil.Types
	}

	// ForwarderOption configures a [Forwarder] at construction time.
	ForwarderOption func(*Forwarder)

	// methodInfo is the resolved descriptor and message types for one full method.
	methodInfo struct {
		desc protoreflect.MethodDescriptor
		in   protoreflect.MessageType
		out  protoreflect.MessageType
	}
)

// NewForwarder builds a Forwarder that forwards over cc every method belonging
// to a service a admits. It fails when cc or a is nil. By default methods are
// typed against the global proto registry; use [WithProtoTypes] to override it.
func NewForwarder(cc grpc.ClientConnInterface, a services.Allowlist, opts ...ForwarderOption) (*Forwarder, error) {
	if cc == nil {
		return nil, fmt.Errorf("proxy: nil client connection passed to forwarder")
	}

	if a == nil {
		return nil, fmt.Errorf("proxy: nil allowlist passed to forwarder")
	}

	f := &Forwarder{
		cc:      cc,
		allowed: a,
		types:   protoregistry.GlobalTypes,
	}

	for _, opt := range opts {
		opt(f)
	}

	return f, nil
}

// WithProtoTypes sets the registry used to resolve a method's request and
// response message types. A nil registry leaves the default in place.
func WithProtoTypes(t protoutil.Types) ForwarderOption {
	return func(f *Forwarder) {
		if t != nil {
			f.types = t
		}
	}
}

// Handle forwards one stream to the upstream, and suits
// [google.golang.org/grpc.UnknownServiceHandler]. A method whose service the
// [services.Allowlist] does not admit is rejected with Unimplemented before any
// upstream work, so the proxy answers as a server that does not implement it
// rather than revealing that an upstream might. Only methods present in the
// compiled descriptors can be forwarded; anything else is Unimplemented too.
func (f *Forwarder) Handle(_ any, ss grpc.ServerStream) error {
	ctx := ss.Context()
	method, err := rpc.FullMethod(ctx)
	if err != nil {
		return err
	}

	if service := rpc.Service(method); !f.allowed.Allows(service) {
		return status.Errorf(codes.Unimplemented, "unknown service %s", service)
	}

	info := f.lookup(method)
	if info == nil {
		return status.Errorf(codes.Unimplemented, "unknown method %s", method)
	}

	ctx = forwardContext(ctx)
	if info.desc.IsStreamingClient() || info.desc.IsStreamingServer() {
		return f.stream(ctx, method, info, ss)
	}

	return f.unary(ctx, method, info, ss)
}

// unary forwards a single request and response, relaying the upstream's response
// header and trailer to the caller.
func (f *Forwarder) unary(ctx context.Context, method string, mi *methodInfo, ss grpc.ServerStream) error {
	req := mi.in.New().Interface()
	if err := ss.RecvMsg(req); err != nil {
		// io.EOF here means the caller half-closed without sending a request, which
		// is a client error rather than a clean end of stream.
		return rpc.StatusError("proxy: reading the request failed", err)
	}

	// Invoke discards the upstream's response metadata unless it is asked for it,
	// so collect both and relay them the way the streaming path does.
	var header, trailer metadata.MD
	resp := mi.out.New().Interface()
	callErr := f.cc.Invoke(ctx, method, req, resp, grpc.Header(&header), grpc.Trailer(&trailer))

	// The trailer carries the upstream's typed error details, so it propagates on
	// failure as well as success and must be set before this handler returns.
	if len(trailer) > 0 {
		ss.SetTrailer(trailer)
	}

	if callErr != nil {
		return callErr
	}

	if len(header) > 0 {
		if err := ss.SetHeader(header); err != nil {
			return rpc.StatusError("proxy: relaying the response header failed", err)
		}
	}

	if err := ss.SendMsg(resp); err != nil {
		return rpc.StatusError("proxy: sending the response failed", err)
	}

	return nil
}

// stream forwards a streaming method, pumping typed messages in both directions
// and propagating the upstream's header, trailer, and status.
func (f *Forwarder) stream(ctx context.Context, method string, mi *methodInfo, ss grpc.ServerStream) error {
	cs, err := f.cc.NewStream(
		ctx,
		&grpc.StreamDesc{
			ServerStreams: mi.desc.IsStreamingServer(),
			ClientStreams: mi.desc.IsStreamingClient(),
		},
		method,
	)
	if err != nil {
		return err
	}

	return rpc.NewPump(cs, ss).Forward(
		func() any { return mi.in.New().Interface() },
		func() any { return mi.out.New().Interface() },
	)
}

// lookup returns the cached methodInfo for fullMethod, resolving and caching it
// on first use. It returns nil when the method cannot be resolved, which is not
// cached so an unresolvable name cannot grow the cache.
func (f *Forwarder) lookup(fullMethod string) *methodInfo {
	if v, ok := f.methods.Load(fullMethod); ok {
		return v.(*methodInfo)
	}

	mi := f.resolveMethod(fullMethod)
	if mi == nil {
		return nil
	}

	actual, _ := f.methods.LoadOrStore(fullMethod, mi)
	return actual.(*methodInfo)
}

// resolveMethod resolves fullMethod to its descriptor and message types, or nil
// when the service is not linked into the binary, the service has no such
// method, or either message type is missing from the registry.
func (f *Forwarder) resolveMethod(fullMethod string) *methodInfo {
	service, method, ok := rpc.ServiceMethod(fullMethod)
	if !ok {
		return nil
	}

	sd, err := services.Resolve(service)
	if err != nil {
		return nil
	}

	md := sd.Methods().ByName(protoreflect.Name(method))
	if md == nil {
		return nil
	}

	in, err := f.types.FindMessageByName(md.Input().FullName())
	if err != nil {
		return nil
	}

	out, err := f.types.FindMessageByName(md.Output().FullName())
	if err != nil {
		return nil
	}

	return &methodInfo{desc: md, in: in, out: out}
}

// forwardContext turns the inbound request's metadata into outgoing metadata for
// the upstream call, minus the transport headers, without displacing a value
// already set on the outgoing context. Templated upstream resolution reads the
// router-stamped namespace from there, so this is load-bearing rather than
// merely polite.
func forwardContext(ctx context.Context) context.Context {
	incoming, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return ctx
	}

	incoming = incoming.Copy()
	for _, key := range transportHeaders {
		incoming.Delete(key)
	}

	if len(incoming) == 0 {
		return ctx
	}

	return rpc.WithOutgoing(ctx, func(outgoing metadata.MD) {
		for k, v := range incoming {
			if len(outgoing.Get(k)) == 0 {
				outgoing.Set(k, v...)
			}
		}
	})
}
