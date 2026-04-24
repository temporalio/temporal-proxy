package proxy

import (
	"context"
	"errors"
	"io"

	adminservicev1 "go.temporal.io/server/api/adminservice/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func forwardUnary[Req, Resp any](
	ctx context.Context,
	req Req,
	fn func(context.Context, Req, ...grpc.CallOption) (Resp, error),
) (Resp, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	return fn(metadata.NewOutgoingContext(ctx, md), req)
}

// forwardBidiStream proxies the AdminService StreamWorkflowReplicationMessages bidi stream.
// It opens an upstream client stream, then pumps messages in both directions concurrently.
// Returns nil on clean EOF, or the first non-EOF error from either direction.
func forwardBidiStream(
	serverStream adminservicev1.AdminService_StreamWorkflowReplicationMessagesServer,
	openStream func(context.Context, ...grpc.CallOption) (adminservicev1.AdminService_StreamWorkflowReplicationMessagesClient, error),
) error {
	md, _ := metadata.FromIncomingContext(serverStream.Context())
	cstream, err := openStream(metadata.NewOutgoingContext(serverStream.Context(), md))
	if err != nil {
		return status.Errorf(codes.Internal, "failed to open upstream stream: %v", err)
	}

	errCh := make(chan error, 2)

	// server → upstream
	go func() {
		for {
			req, err := serverStream.Recv()
			if err != nil {
				_ = cstream.CloseSend()
				errCh <- err
				return
			}

			if err := cstream.Send(req); err != nil {
				errCh <- err
				return
			}
		}
	}()

	// upstream → server
	go func() {
		for {
			resp, err := cstream.Recv()
			if err != nil {
				errCh <- err
				return
			}

			if err := serverStream.Send(resp); err != nil {
				errCh <- err
				return
			}
		}
	}()

	// Collect errors from both goroutines; return the first non-EOF error.
	var firstErr error
	for range 2 {
		if err := <-errCh; !errors.Is(err, io.EOF) && firstErr == nil {
			firstErr = err
		}
	}

	return firstErr
}
