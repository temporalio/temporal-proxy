package router

import (
	"errors"
	"io"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	"github.com/temporalio/temporal-proxy/internal/rpc"
	"github.com/temporalio/temporal-proxy/internal/transport/meta"
)

// ForwardTo returns a handler that forwards a routed stream over cc using the
// same full method name: it replays the first frame, pumps raw frames in both
// directions, and propagates header, trailer, and status verbatim. The caller's
// metadata goes with it, with the routed namespace laid over it.
//
// Test only: production routes each upstream to its forwarder in process. The
// handler tests use this to drive Handler against a plain upstream server.
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
