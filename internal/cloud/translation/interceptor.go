package translation

import (
	"context"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"

	"github.com/temporalio/temporal-proxy/internal/rpc"
)

// Option configures the interceptor [DialOptions] installs.
type Option func(*options)

type options struct {
	via grpc.ClientConnInterface
}

// Via sends a translated call over cc instead of continuing down the chain to
// the connection the interceptor is installed on.
//
// The upstream method belongs to a different service, which the connection that
// received the call does not serve: the caller asked a Temporal Service for
// ListNamespaces, and only Temporal Cloud's control plane can answer it. Where
// that service lives is as fixed as the conversions themselves, so the
// translation carries the connection rather than the request being routed to it,
// and a request reaching any upstream is answered the same way.
func Via(cc grpc.ClientConnInterface) Option {
	return func(o *options) { o.via = cc }
}

// DialOptions returns the dial options that install method translation on an
// outbound connection. Callers fold them into the dial options for the upstream
// connection, last, so translation is the innermost interceptor: every other
// interceptor on the chain then sees the method and message types the caller
// asked for rather than the substitute sent upstream.
//
// The result is a slice though it holds a single option today. A [Translation]
// substitutes a unary method, so a unary interceptor is all there is to install;
// translating a streaming method would add a stream interceptor beside it, the
// way the namespace and Cloud-namespace helpers in internal/proxy already pair
// the two. Keeping the slice means that arrives without changing this signature
// or the call sites, which already spread the result.
func DialOptions(r *Registry, opts ...Option) []grpc.DialOption {
	return []grpc.DialOption{grpc.WithChainUnaryInterceptor(unaryClientInterceptor(r, opts...))}
}

// unaryClientInterceptor returns a unary client interceptor that replaces a call
// to a method r translates with a call to the method it translates onto: the
// request is converted, invoked under the upstream method, and the upstream's
// reply is folded into the reply the caller allocated. A method r does not
// translate is invoked unchanged, as is a call whose request or reply is not a
// proto message. Any headers the translation declares are stamped on the
// substituted call only.
//
// An upstream error is returned as it arrived, so the caller sees the upstream's
// status rather than a translated one. A conversion that fails becomes Internal
// via [rpc.StatusError]: the mapping is compiled in, so a failure there is a
// proxy bug rather than anything the caller did.
func unaryClientInterceptor(r *Registry, opts ...Option) grpc.UnaryClientInterceptor {
	o := &options{}
	for _, opt := range opts {
		opt(o)
	}

	return func(
		ctx context.Context,
		method string,
		req, reply any,
		cc *grpc.ClientConn,
		invoker grpc.UnaryInvoker,
		callOpts ...grpc.CallOption,
	) error {
		t, ok := r.Lookup(method)
		if !ok {
			return invoker(ctx, method, req, reply, cc, callOpts...)
		}

		in, inOK := req.(proto.Message)
		out, outOK := reply.(proto.Message)
		if !inOK || !outOK {
			// Nothing to convert against. The forwarder types every message from the
			// proto registry, so this only happens on a hand-rolled call; forwarding
			// it unchanged is closer to right than failing it.
			return invoker(ctx, method, req, reply, cc, callOpts...)
		}

		upReq, err := t.request(in)
		if err != nil {
			return rpc.StatusError("translation: adapting the request failed", err)
		}

		upReply := t.reply()
		if err := o.invoke(t.stamp(ctx), t, upReq, upReply, cc, invoker, callOpts...); err != nil {
			return err
		}

		if err := t.response(in, upReply, out); err != nil {
			return rpc.StatusError("translation: adapting the reply failed", err)
		}

		return nil
	}
}

// invoke sends the substituted call: over the connection Via named when there is
// one, and otherwise down the chain to the connection the interceptor was
// installed on.
func (o *options) invoke(
	ctx context.Context,
	t *Translation,
	req, reply any,
	cc *grpc.ClientConn,
	invoker grpc.UnaryInvoker,
	callOpts ...grpc.CallOption,
) error {
	if o.via != nil {
		return o.via.Invoke(ctx, t.to, req, reply, callOpts...)
	}

	return invoker(ctx, t.to, req, reply, cc, callOpts...)
}
