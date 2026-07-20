package auth

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

func TestStripOutgoingUnary(t *testing.T) {
	t.Parallel()

	ctx := metadata.NewOutgoingContext(t.Context(), metadata.Pairs("authorization", "Bearer forwarded", "x-keep", "v"))

	var gotCtx context.Context
	invoker := func(ic context.Context, _ string, _, _ any, _ *grpc.ClientConn, _ ...grpc.CallOption) error {
		gotCtx = ic
		return nil
	}

	err := stripOutgoingUnary("authorization")(ctx, "/svc/M", nil, nil, nil, invoker)
	require.NoError(t, err)

	md, _ := metadata.FromOutgoingContext(gotCtx)
	require.Empty(t, md.Get("authorization"), "the credential header must be stripped from forwarded metadata")
	require.Equal(t, []string{"v"}, md.Get("x-keep"), "other forwarded headers must be preserved")
}
