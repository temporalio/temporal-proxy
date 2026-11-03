package dataplane

import (
	"context"
	"errors"
	"fmt"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"go.temporal.io/proxy/internal/router"
	"go.temporal.io/proxy/internal/rpc"
	"go.temporal.io/proxy/internal/services"
	"go.temporal.io/proxy/internal/transport/meta"
)

type (
	// namespaceConn is an upstream connection bound to one local namespace. It
	// refuses a method whose service the dataplane does not allow and a request
	// that does not name the bound namespace, and sets the bound namespace on
	// each call's outgoing metadata for both [meta.NamespaceHeader] and
	// [meta.TemporalNamespaceHeader], replacing any value the caller sent.
	namespaceConn struct {
		cc        grpc.ClientConnInterface
		namespace string
		allowlist services.Allowlist
	}

	// namespaceStream checks each message sent on a stream opened through a
	// namespaceConn.
	namespaceStream struct {
		grpc.ClientStream

		namespace string
	}

	// namespaced is a request that names its namespace, as every namespaced
	// Temporal request does. It mirrors the getter protoutil's extractor reads.
	namespaced interface {
		GetNamespace() string
	}
)

// ConnFor returns the upstream connection for namespace: the same connection,
// and so the same namespace translation, upstream credentials, Cloud
// translations and payload codecs, that the per-upstream proxy applies to a
// gateway request for namespace. The upstream is chosen by routing namespace
// with no request metadata, so a rule that matches on metadata never selects
// it, and the default upstream serves it when no namespace rule does. It fails
// when namespace is empty or nothing routes it.
//
// A call is checked before it reaches the upstream. A method whose service the
// allowlist passed to [WithAllowlist] does not admit is rejected with
// Unimplemented, as the gateway rejects it. A request whose namespace field is
// empty or names another namespace is rejected with InvalidArgument, since its
// payloads would be bound to the wrong namespace, and so is a request type with
// no namespace field at all (GetSystemInfo, ListNamespaces and the like), which
// the gateway would not route by namespace. On a stream, each sent message is
// checked the same way.
//
// Outgoing metadata on the call's context travels to the upstream, except that
// any [meta.NamespaceHeader] and [meta.TemporalNamespaceHeader] values are
// replaced with namespace, as the router and an SDK set them on a gateway
// request. The per-upstream proxy then translates the latter like a gateway
// request's, and the former selects a templated upstream's target per call.
// The gateway's request metrics and routing decisions are not recorded, since
// no request passes through either.
//
// The connection is safe for concurrent use. Calls are meant to start once
// Start has returned, which opens static upstream connections ahead of the
// first call. Stop leaves it untouched: it fails once the connection pool
// passed to [WithPool] closes, so callers must stop using it before then. Under
// fx, a caller that depends on the pool is stopped before the pool closes.
// Calls bypass the gateway's panic recovery, so a caller such as an HTTP layer
// must recover panics itself.
func (d *Dataplane) ConnFor(namespace string) (grpc.ClientConnInterface, error) {
	if namespace == "" {
		return nil, errors.New("dataplane: a namespace is required")
	}

	upstream, outcome := d.mux.Switch(namespace, nil)
	if outcome == router.OutcomeUnroutable {
		return nil, fmt.Errorf("dataplane: no upstream routes namespace %q and no default is configured", namespace)
	}

	cc, ok := d.conns[upstream]
	if !ok {
		return nil, fmt.Errorf("dataplane: namespace %q routes to unknown upstream %q", namespace, upstream)
	}

	return &namespaceConn{cc: cc, namespace: namespace, allowlist: d.allowlist}, nil
}

// Invoke checks method's service and req's namespace and performs the call with
// the bound namespace stamped on ctx.
func (c *namespaceConn) Invoke(ctx context.Context, method string, req, reply any, opts ...grpc.CallOption) error {
	if err := c.checkService(method); err != nil {
		return err
	}

	if err := checkNamespace(req, c.namespace); err != nil {
		return err
	}

	return c.cc.Invoke(c.context(ctx), method, req, reply, opts...)
}

// NewStream checks method's service and opens a stream with the bound namespace
// stamped on ctx, whose sent messages are checked like a unary request.
func (c *namespaceConn) NewStream(
	ctx context.Context,
	desc *grpc.StreamDesc,
	method string,
	opts ...grpc.CallOption,
) (grpc.ClientStream, error) {
	if err := c.checkService(method); err != nil {
		return nil, err
	}

	cs, err := c.cc.NewStream(c.context(ctx), desc, method, opts...)
	if err != nil {
		return nil, err
	}

	return &namespaceStream{ClientStream: cs, namespace: c.namespace}, nil
}

// checkService rejects method with Unimplemented when the allowlist does not
// admit its service, as [router.Handler] does.
func (c *namespaceConn) checkService(method string) error {
	if svc := rpc.Service(method); !c.allowlist.Allows(svc) {
		return status.Errorf(codes.Unimplemented, "unknown service %q", svc)
	}

	return nil
}

// context returns ctx with the bound namespace set on its outgoing metadata for
// both [meta.NamespaceHeader] and [meta.TemporalNamespaceHeader], replacing any
// value the caller sent for either.
func (c *namespaceConn) context(ctx context.Context) context.Context {
	return rpc.WithOutgoing(ctx, func(md metadata.MD) {
		md.Set(meta.NamespaceHeader, c.namespace)
		md.Set(meta.TemporalNamespaceHeader, c.namespace)
	})
}

// SendMsg checks m's namespace before sending it.
func (s *namespaceStream) SendMsg(m any) error {
	if err := checkNamespace(m, s.namespace); err != nil {
		return err
	}

	return s.ClientStream.SendMsg(m)
}

// checkNamespace rejects a request that does not name ns, including one whose
// type has no namespace field.
func checkNamespace(req any, ns string) error {
	n, ok := req.(namespaced)
	if !ok {
		return status.Errorf(
			codes.InvalidArgument,
			"dataplane: a %T names no namespace, so a connection for namespace %q cannot carry it",
			req,
			ns,
		)
	}

	if n.GetNamespace() != ns {
		return status.Errorf(
			codes.InvalidArgument,
			"dataplane: request names namespace %q on a connection for namespace %q",
			n.GetNamespace(),
			ns,
		)
	}

	return nil
}
