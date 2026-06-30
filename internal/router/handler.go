package router

import (
	"io"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// Handler returns a grpc.StreamHandler suitable for grpc.UnknownServiceHandler.
// It transparently forwards every stream to cc using the same full method name,
// pumping raw frames in both directions and propagating header, trailer, and
// status verbatim.
//
// cc is resolved once per stream, which is the seam where future per-request
// routing (selecting a connection from request details) will plug in.
func Handler(cc *grpc.ClientConn) grpc.StreamHandler {
	return func(_ any, serverStream grpc.ServerStream) error {
		ctx := serverStream.Context()

		sts := grpc.ServerTransportStreamFromContext(ctx)
		if sts == nil {
			return status.Error(codes.Internal, "router: no server transport stream in context")
		}
		method := sts.Method()

		outCtx := ctx
		if md, ok := metadata.FromIncomingContext(ctx); ok {
			outCtx = metadata.NewOutgoingContext(ctx, md.Copy())
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
