package proxy

import (
	"context"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

//go:generate go tool -modfile=../../dev/tools.mod mockgen -destination workflow_mocks_test.go -package proxy_test -typed go.temporal.io/api/workflowservice/v1 WorkflowServiceClient
//go:generate go run github.com/temporalio/temporal-proxy/cmd/codegen proxy -o workflow_service.go -t WorkflowService

// forward relays a unary RPC to the upstream client via fn, copying any inbound
// gRPC metadata from the incoming context onto the outgoing context so it
// reaches the upstream. It is the single point of delegation shared by every
// generated proxy method.
func forward[Req, Resp any](
	ctx context.Context,
	req Req,
	fn func(context.Context, Req, ...grpc.CallOption) (Resp, error),
) (Resp, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	return fn(metadata.NewOutgoingContext(ctx, md), req)
}
