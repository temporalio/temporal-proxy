package proxy

import (
	"context"
	"io"
	"strings"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"

	"github.com/temporalio/temporal-proxy/internal/services"
)

// transportHeaders are set by the transport on every call (user-agent and
// content-type end-to-end, :authority as an HTTP/2 pseudo-header), so
// forwarding the caller's copies would duplicate or override the ones this
// proxy's own client produces.
var transportHeaders = []string{"user-agent", ":authority", "content-type"}

type (
	// Forwarder serves any gRPC method whose service is admitted and whose
	// descriptors are linked into the binary, decoding it into its concrete
	// request type and reinvoking it on the upstream connection.
	//
	// Decoding is what makes the interceptor chain work: namespace translation
	// and payload encryption operate on typed messages, so a raw relay would
	// forward successfully while silently skipping both. A method that cannot be
	// typed is refused for the same reason.
	//
	// Admission is a second, independent reason to refuse a call: this
	// Forwarder sits behind a unix socket, and without its own check it would
	// serve any linked service to anything that can dial that socket,
	// regardless of what the gateway's own allowedServices gate admits.
	Forwarder struct {
		cc      grpc.ClientConnInterface
		allowed serviceSet
		methods sync.Map // fullMethod -> *methodInfo, successful resolutions only; see lookup.
	}

	// methodInfo is the resolved shape of one method: its descriptor, which says
	// whether the method streams, and the request and response types to allocate.
	methodInfo struct {
		desc protoreflect.MethodDescriptor
		in   protoreflect.MessageType
		out  protoreflect.MessageType
	}

	// serviceSet is the Forwarder's own minimal admission check. It is not
	// router.Gate: internal/proxy must not import internal/router, since the
	// gateway dials this proxy's socket and an import the other way would
	// invert that topology. The two types are small enough that duplicating the
	// shape here is the right call rather than sharing it.
	serviceSet map[string]struct{}
)

// NewForwarder returns a Forwarder that reinvokes calls on cc for any service
// named in allowed, refusing every other service with the same Unimplemented
// shape the gateway's gate uses, so a caller cannot tell the two hops apart.
// allowed is expanded with each service's compatibility alias (see
// services.Expand), matching what the gateway admits, so a v1alpha reflection
// call the gateway let through is not refused again here for want of its own
// alias entry.
//
// cc is typically the resolving connection for one upstream, so the
// interceptors installed on it apply to everything the Forwarder carries.
func NewForwarder(cc grpc.ClientConnInterface, allowed []string) *Forwarder {
	return &Forwarder{cc: cc, allowed: newServiceSet(allowed)}
}

// Handle forwards one call, shaped for grpc.UnknownServiceHandler. It refuses a
// service the Forwarder does not admit, then reports Unimplemented for a method
// whose descriptors are not linked in, which is what an upstream newer than
// this binary looks like.
func (f *Forwarder) Handle(_ any, ss grpc.ServerStream) error {
	ctx := ss.Context()
	sts := grpc.ServerTransportStreamFromContext(ctx)
	if sts == nil {
		return status.Error(codes.Internal, "proxy: no server transport stream in context")
	}

	method := sts.Method()
	if service := serviceOf(method); !f.allowed.allows(service) {
		return status.Errorf(codes.Unimplemented, "unknown service %s", service)
	}

	mi := f.lookup(method)
	if mi == nil {
		return status.Errorf(codes.Unimplemented, "unknown method %s", method)
	}

	outCtx := forwardContext(ctx)

	if mi.desc.IsStreamingClient() || mi.desc.IsStreamingServer() {
		return f.stream(outCtx, method, mi, ss)
	}

	return f.unary(outCtx, method, mi, ss)
}

// unary decodes the request, reinvokes it upstream so the interceptor chain
// sees a typed message, and sends the reply back. An upstream error is returned
// as-is: it already carries a gRPC status, and the translation interceptor has
// already rewritten any namespace in its details.
//
// The reply is sent without forwarding any upstream response headers or
// trailers. That matches the generated server this replaces, which did not
// forward them either; it is intentional scope, not an oversight, so it
// should not be "fixed" here.
func (f *Forwarder) unary(ctx context.Context, method string, mi *methodInfo, ss grpc.ServerStream) error {
	req := mi.in.New().Interface()
	if err := ss.RecvMsg(req); err != nil {
		return err
	}

	resp := mi.out.New().Interface()
	if err := f.cc.Invoke(ctx, method, req, resp); err != nil {
		return err
	}

	return ss.SendMsg(resp)
}

// stream forwards a streaming call frame by frame, typed in both directions so
// the interceptor chain sees decoded messages. Header, trailer, and status
// propagate verbatim: unlike unary, a stream is a transparent relay for its
// whole lifetime, and the timing of its header and trailer is itself
// observable to the caller, so there is no generated-server behavior to match
// by omitting them.
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

	reqErr := pumpRequests(ss, cs, mi.in)
	respErr := pumpResponses(cs, ss, mi.out)

	for range 2 {
		select {
		case err := <-reqErr:
			if err == io.EOF {
				_ = cs.CloseSend()
				continue
			}

			return statusError(err)
		case err := <-respErr:
			ss.SetTrailer(cs.Trailer())
			if err != io.EOF {
				return err
			}

			return nil
		}
	}

	// Defensive: pumpResponses always returns above; unreachable in normal flow.
	return status.Error(codes.Internal, "proxy: forwarding ended without completion")
}

// lookup returns the resolved method, or nil when it cannot be resolved. Only
// successful resolutions are cached: the method portion of fullMethod is
// caller-controlled and the gateway does not validate it, so caching misses
// would let a client grow the cache without bound by probing method names.
// Successful resolutions are bounded by the linked descriptor set, so caching
// them is safe. Two concurrent first calls for the same method may both
// resolve; LoadOrStore makes them agree on one cached result rather than each
// keeping its own.
func (f *Forwarder) lookup(fullMethod string) *methodInfo {
	if v, ok := f.methods.Load(fullMethod); ok {
		return v.(*methodInfo)
	}

	mi := resolveMethod(fullMethod)
	if mi == nil {
		return nil
	}

	actual, _ := f.methods.LoadOrStore(fullMethod, mi)
	return actual.(*methodInfo)
}

// allows reports whether the set admits service.
func (s serviceSet) allows(service string) bool {
	_, ok := s[service]
	return ok
}

// forwardContext copies incoming metadata onto the outgoing context, minus the
// headers the transport sets itself, without overwriting a value the outgoing
// context already carries.
//
// This is what carries the router-stamped namespace and the caller's headers to
// the upstream, so templated upstream resolution and namespace translation both
// depend on it.
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

	outgoing, ok := metadata.FromOutgoingContext(ctx)
	if !ok {
		outgoing = metadata.MD{}
	} else {
		outgoing = outgoing.Copy()
	}

	for k, v := range incoming {
		if len(outgoing.Get(k)) == 0 {
			outgoing.Set(k, v...)
		}
	}

	return metadata.NewOutgoingContext(ctx, outgoing)
}

// resolveMethod resolves "/pkg.Service/Method" to its descriptor and message
// types, returning nil when the name is malformed or anything is unregistered.
//
// The service descriptor comes from services.Resolve rather than reading
// protoregistry.GlobalFiles directly, so the dependency on internal/services
// (and the descriptor-registering imports it carries) is explicit. Without
// that explicit import, the descriptors this package relies on would arrive
// only by accident of what else happens to be linked in, and a later cleanup
// elsewhere could silently break every call for a service this package still
// claims to forward.
func resolveMethod(fullMethod string) *methodInfo {
	trimmed := strings.TrimPrefix(fullMethod, "/")
	slash := strings.LastIndex(trimmed, "/")
	if slash < 0 {
		return nil
	}

	sd, err := services.Resolve(trimmed[:slash])
	if err != nil {
		return nil
	}

	md := sd.Methods().ByName(protoreflect.Name(trimmed[slash+1:]))
	if md == nil {
		return nil
	}

	in, err := protoregistry.GlobalTypes.FindMessageByName(md.Input().FullName())
	if err != nil {
		return nil
	}

	out, err := protoregistry.GlobalTypes.FindMessageByName(md.Output().FullName())
	if err != nil {
		return nil
	}

	return &methodInfo{desc: md, in: in, out: out}
}

// serviceOf returns the service portion of a full gRPC method name
// ("/pkg.Service/Method"), or "" when the name is malformed. It mirrors
// router.ServiceOf; this package does not import internal/router to reuse it,
// since the gateway dials this proxy's socket and an import the other way
// would invert that topology.
func serviceOf(fullMethod string) string {
	trimmed := strings.TrimPrefix(fullMethod, "/")
	slash := strings.LastIndex(trimmed, "/")
	if slash < 0 {
		return ""
	}

	return trimmed[:slash]
}

// newServiceSet builds the admission set from names, expanding each with its
// compatibility alias so a caller admitting reflection does not also have to
// name its alias (see services.Expand).
func newServiceSet(names []string) serviceSet {
	expanded := services.Expand(names)
	set := make(serviceSet, len(expanded))
	for _, name := range expanded {
		set[name] = struct{}{}
	}

	return set
}

// pumpRequests forwards caller frames upstream, allocating a fresh message per
// frame so a slow upstream cannot observe the next request written over the one
// it is still reading.
func pumpRequests(src grpc.ServerStream, dst grpc.ClientStream, in protoreflect.MessageType) <-chan error {
	ret := make(chan error, 1)
	go func() {
		for {
			msg := in.New().Interface()
			if err := src.RecvMsg(msg); err != nil {
				ret <- err // io.EOF on clean half-close.
				return
			}
			if err := dst.SendMsg(msg); err != nil {
				ret <- err
				return
			}
		}
	}()

	return ret
}

// pumpResponses forwards upstream frames to the caller. The header is forwarded
// once up front: Header blocks until the upstream sends one or the stream
// completes, so header-only and immediately-failing responses still propagate.
func pumpResponses(src grpc.ClientStream, dst grpc.ServerStream, out protoreflect.MessageType) <-chan error {
	ret := make(chan error, 1)
	go func() {
		md, err := src.Header()
		if err != nil {
			ret <- err
			return
		}
		if err := dst.SendHeader(md); err != nil {
			ret <- err
			return
		}

		for {
			msg := out.New().Interface()
			if err := src.RecvMsg(msg); err != nil {
				ret <- err // io.EOF on clean completion, else the upstream status.
				return
			}
			if err := dst.SendMsg(msg); err != nil {
				ret <- err
				return
			}
		}
	}()

	return ret
}

// statusError maps a pump error to the status returned to the caller,
// forwarding one that already carries a gRPC status and mapping a raw context
// error rather than masking either as Internal.
func statusError(err error) error {
	if _, ok := status.FromError(err); ok {
		return err
	}

	if st := status.FromContextError(err); st.Code() != codes.Unknown {
		return st.Err()
	}

	return status.Errorf(codes.Internal, "proxy: request stream failed: %v", err)
}
