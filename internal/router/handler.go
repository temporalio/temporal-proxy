package router

import (
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
	// chosen upstream's name (always non-empty) and the connection to forward the
	// stream over. On a non-nil error the Target is unused and callers must ignore
	// its fields.
	Target struct {
		Upstream string
		Conn     grpc.ClientConnInterface
	}
)

// Handler returns a grpc.StreamHandler suitable for grpc.UnknownServiceHandler,
// reporting a stream_setup forwarding error via rep when opening the upstream
// stream fails. A method whose service a does not allow is rejected with
// Unimplemented before any upstream work, so the proxy answers as a server that
// does not implement it rather than revealing that an upstream might.
// It reads the request namespace from the [meta.Target] that [PeekInterceptor]
// resolved, asks d for the upstream connection, then transparently forwards the
// stream to that upstream using the same full method name: it replays the first
// frame, pumps raw frames in both directions, and propagates header, trailer, and
// status verbatim.
func Handler(d Director, a services.Allowlist, rep *Reporter) grpc.StreamHandler {
	return func(_ any, serverStream grpc.ServerStream) error {
		ctx := serverStream.Context()
		method, err := rpc.FullMethod(ctx)
		if err != nil {
			return err
		}

		if svc := rpc.Service(method); !a.Allows(svc) {
			return status.Errorf(codes.Unimplemented, "unknown service %q", svc)
		}

		var md map[string][]string
		outCtx := ctx
		if inMD, ok := metadata.FromIncomingContext(ctx); ok {
			outCtx = metadata.NewOutgoingContext(ctx, inMD.Copy())
			md = inMD
		}

		// An absent Target means PeekInterceptor did not run, so the namespace is
		// unknown rather than empty. Routing on the difference would quietly send
		// every request to the default upstream, so say so instead.
		peeked := meta.TargetFrom(ctx)
		if peeked.FullName == "" {
			return status.Error(codes.Internal, "router: no target on the request context")
		}

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

		// Carry the extracted namespace to the upstream proxy so it can resolve a
		// templated address without re-parsing the payload. Set (not append) so a
		// client-supplied value cannot influence routing.
		outCtx = meta.WithNamespace(outCtx, peeked.Namespace)

		target, err := d.Resolve(ctx, method, peeked.Namespace, maps.Clone(md))
		if err != nil {
			return err
		}

		stream, err := target.Conn.NewStream(
			outCtx,
			&grpc.StreamDesc{ServerStreams: true, ClientStreams: true},
			method,
			grpc.ForceCodecV2(Codec()),
		)
		if err != nil {
			if rep != nil {
				rep.ForwardingError(target.Upstream, reasonStreamSetup)
			}

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
