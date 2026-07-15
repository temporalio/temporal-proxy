package router

import (
	"context"
	"io"
	"maps"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type (
	// Director selects the upstream connection for a request. Resolve receives
	// the full method, the namespace peeked from the first request message
	// (empty when the client sent no message), and the incoming metadata, and
	// returns the connection to forward the stream over. A non-nil error aborts
	// the stream and is returned to the caller verbatim, so implementations
	// should return a gRPC status error.
	Director interface {
		Resolve(ctx context.Context, method, namespace string, md map[string][]string) (*grpc.ClientConn, error)
	}

	// Reflector extracts the Temporal namespace from a request. Namespace
	// receives the full method and the raw bytes of the first request message
	// and returns the namespace, or "" when it cannot determine one.
	Reflector interface {
		Namespace(string, []byte) string
	}
)

// Handler returns a grpc.StreamHandler suitable for grpc.UnknownServiceHandler.
// It buffers the first request frame so r can peek the request namespace, asks d
// for the upstream connection, then transparently forwards the stream to that
// upstream using the same full method name: it replays the buffered first frame,
// pumps raw frames in both directions, and propagates header, trailer, and
// status verbatim.
func Handler(d Director, r Reflector) grpc.StreamHandler {
	return func(_ any, serverStream grpc.ServerStream) error {
		ctx := serverStream.Context()
		sts := grpc.ServerTransportStreamFromContext(ctx)
		if sts == nil {
			return status.Error(codes.Internal, "router: no server transport stream in context")
		}

		var md map[string][]string
		method := sts.Method()
		outCtx := ctx
		if inMD, ok := metadata.FromIncomingContext(ctx); ok {
			outCtx = metadata.NewOutgoingContext(ctx, inMD.Copy())
			md = inMD
		}

		// Buffer the first client frame so we can read the namespace before
		// choosing an upstream. io.EOF means the client half-closed without
		// sending a message (namespace is empty).
		first := &frame{}
		firstErr := serverStream.RecvMsg(first)
		eof := firstErr == io.EOF
		if firstErr != nil && !eof {
			return StatusError(firstErr)
		}

		namespace := ""
		if !eof {
			namespace = r.Namespace(method, first.payload)
		}

		cc, err := d.Resolve(ctx, method, namespace, maps.Clone(md))
		if err != nil {
			return err
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
				return StatusError(err)
			}
		} else if err := stream.SendMsg(first); err != nil {
			return StatusError(err)
		}

		reqErr := pumpServerToClient(serverStream, stream)
		respErr := pumpClientToServer(stream, serverStream)

		for range 2 {
			select {
			case err := <-reqErr:
				if err == io.EOF {
					_ = stream.CloseSend()
					continue
				}

				// Preserve the caller's gRPC/context status instead of masking it as Internal.
				return StatusError(err)
			case err := <-respErr:
				serverStream.SetTrailer(stream.Trailer())
				if err != io.EOF {
					return err
				}

				return nil
			}
		}

		// Defensive: respErr always returns above; this is unreachable in normal flow.
		return status.Error(codes.Internal, "router: forwarding ended without completion")
	}
}

// StatusError maps a request-pump error to the gRPC status returned to the
// caller. It forwards an error that already carries a gRPC status verbatim, maps a
// raw context error to its status, and otherwise reports Internal.
func StatusError(err error) error {
	if _, ok := status.FromError(err); ok {
		return err
	}

	if st := status.FromContextError(err); st.Code() != codes.Unknown {
		return st.Err()
	}

	return status.Errorf(codes.Internal, "router: request stream failed: %v", err)
}

// pumpServerToClient forwards request frames from the caller to the upstream.
func pumpServerToClient(src grpc.ServerStream, dst grpc.ClientStream) <-chan error {
	ret := make(chan error, 1)
	go func() {
		f := &frame{}
		for {
			if err := src.RecvMsg(f); err != nil {
				ret <- err // io.EOF on clean half-close.
				return
			}
			if err := dst.SendMsg(f); err != nil {
				ret <- err
				return
			}
		}
	}()
	return ret
}

// pumpClientToServer forwards response frames from the upstream to the caller.
// It forwards the response header once up front: Header() blocks until the
// upstream sends headers or the stream completes, so header-only and
// immediately-failing responses still propagate their metadata.
func pumpClientToServer(src grpc.ClientStream, dst grpc.ServerStream) <-chan error {
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

		f := &frame{}
		for {
			if err := src.RecvMsg(f); err != nil {
				ret <- err // io.EOF on clean completion, else the upstream status.
				return
			}
			if err := dst.SendMsg(f); err != nil {
				ret <- err
				return
			}
		}
	}()
	return ret
}
