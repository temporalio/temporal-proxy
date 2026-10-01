package router

import (
	"context"
	"errors"
	"io"
	"maps"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/temporalio/temporal-proxy/internal/rpc"
	"github.com/temporalio/temporal-proxy/internal/services"
	"github.com/temporalio/temporal-proxy/internal/transport/meta"
)

type (
	// Target is the routing result returned by Director.Resolve on success: the
	// chosen upstream's name (always non-empty) and the handler that serves the
	// stream. Handle receives the routed stream, whose outgoing metadata carries
	// the resolved namespace; it must let that value override any the caller
	// sent. On a non-nil error the Target is unused and callers must ignore its
	// fields.
	Target struct {
		Upstream string
		Handle   grpc.StreamHandler
	}

	// routedStream is the gateway's stream as a Target's handler sees it: the same
	// stream, with a context carrying the namespace the router resolved as
	// outgoing metadata.
	routedStream struct {
		grpc.ServerStream
		ctx context.Context
	}
)

// Handler returns a grpc.StreamHandler suitable for grpc.UnknownServiceHandler.
// A method whose service a does not allow is rejected with Unimplemented before
// any upstream work, so the proxy answers as a server that does not implement it
// rather than revealing that an upstream might. It reads the request namespace
// from the [meta.Target] that [PeekInterceptor] resolved, asks d for the
// upstream, and hands the stream to the Target's handler with the first frame
// still buffered and the namespace as outgoing metadata.
func Handler(d Director, a services.Allowlist) grpc.StreamHandler {
	return func(srv any, serverStream grpc.ServerStream) error {
		ctx := serverStream.Context()
		method, err := rpc.FullMethod(ctx)
		if err != nil {
			return err
		}

		if svc := rpc.Service(method); !a.Allows(svc) {
			return status.Errorf(codes.Unimplemented, "unknown service %q", svc)
		}

		// An absent Target means PeekInterceptor did not run, so the namespace is
		// unknown rather than empty. Routing on the difference would quietly send
		// every request to the default upstream, so say so instead.
		peeked := meta.TargetFrom(ctx)
		if peeked.FullName == "" {
			return status.Error(codes.Internal, "router: no target on the request context")
		}

		inMD, _ := metadata.FromIncomingContext(ctx)
		target, err := d.Resolve(ctx, method, peeked.Namespace, maps.Clone(inMD))
		if err != nil {
			return err
		}

		// The namespace travels as outgoing metadata so the handler can carry it on
		// to the upstream without re-parsing the payload. It is set, not appended,
		// and outgoing values win over the caller's, so a client-supplied value
		// cannot influence routing downstream.
		return target.Handle(srv, &routedStream{
			ServerStream: serverStream,
			ctx:          meta.WithNamespace(ctx, peeked.Namespace),
		})
	}
}

// ForwardTo returns a handler that forwards a routed stream over cc using the
// same full method name: it replays the first frame, pumps raw frames in both
// directions, and propagates header, trailer, and status verbatim. The caller's
// metadata goes with it, with the routed namespace laid over it.
func ForwardTo(cc grpc.ClientConnInterface) grpc.StreamHandler {
	return func(_ any, serverStream grpc.ServerStream) error {
		ctx := serverStream.Context()
		method, err := rpc.FullMethod(ctx)
		if err != nil {
			return err
		}

		outCtx := ctx
		if inMD, ok := metadata.FromIncomingContext(ctx); ok {
			outCtx = metadata.NewOutgoingContext(ctx, inMD.Copy())
		}
		outCtx = meta.WithNamespace(outCtx, meta.NamespaceFrom(ctx))

		// Collect the first client frame so it can be replayed upstream. The peek
		// interceptor already took it off the wire and reported any failure reading
		// it, so this returns the buffered bytes rather than touching the transport,
		// and anything other than io.EOF (the client half-closed without sending a
		// message) already carries its own status.
		first := &frame{}
		firstErr := serverStream.RecvMsg(first)
		eof := errors.Is(firstErr, io.EOF)
		if firstErr != nil && !eof {
			return firstErr
		}

		stream, err := cc.NewStream(
			outCtx,
			&grpc.StreamDesc{ServerStreams: true, ClientStreams: true},
			method,
			grpc.ForceCodecV2(Codec()),
		)
		if err != nil {
			return err
		}

		if eof {
			if err := stream.CloseSend(); err != nil {
				return rpc.StatusError("router: closing the upstream send side failed", err)
			}
		} else if err := stream.SendMsg(first); err != nil {
			return rpc.StatusError("router: forwarding the first request failed", err)
		}

		return rpc.NewPump(stream, serverStream).Forward(
			func() any { return &frame{} },
			func() any { return &frame{} },
		)
	}
}

// Context returns the context carrying the resolved namespace.
func (s *routedStream) Context() context.Context { return s.ctx }
