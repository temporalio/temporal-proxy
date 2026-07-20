package auth_test

import (
	"bytes"
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/temporalio/temporal-proxy/internal/auth"
	"github.com/temporalio/temporal-proxy/pkg/logger"
)

type fakeStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (f *fakeStream) Context() context.Context { return f.ctx }

type fakeAuthenticator struct{ err error }

func (f fakeAuthenticator) Authenticate(context.Context, metadata.MD) error { return f.err }

func TestStreamServerInterceptor(t *testing.T) {
	t.Parallel()

	call := func(a auth.Authenticator, md metadata.MD) (called bool, err error) {
		ctx := metadata.NewIncomingContext(t.Context(), md)
		ic := auth.StreamServerInterceptor(a, nil)
		err = ic(nil, &fakeStream{ctx: ctx}, &grpc.StreamServerInfo{}, func(any, grpc.ServerStream) error {
			called = true
			return nil
		})
		return called, err
	}

	t.Run("authenticated request reaches handler", func(t *testing.T) {
		t.Parallel()
		called, err := call(fakeAuthenticator{err: nil}, metadata.Pairs("authorization", "Bearer x"))
		require.NoError(t, err)
		require.True(t, called)
	})

	t.Run("rejected request never reaches handler", func(t *testing.T) {
		t.Parallel()
		want := status.Error(codes.Unauthenticated, "nope")
		called, err := call(fakeAuthenticator{err: want}, nil)
		require.False(t, called)
		require.Equal(t, codes.Unauthenticated, status.Code(err))
	})
}

func TestStreamServerInterceptorLogsReason(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	log := logger.NewZeroLogger(&buf, logger.LevelInfo)

	a, err := auth.NewStaticTokenAuthenticator("s3cret", "", "")
	require.NoError(t, err)

	ic := auth.StreamServerInterceptor(a, log)
	ctx := metadata.NewIncomingContext(t.Context(), metadata.Pairs("authorization", "Bearer wrong"))
	called := false
	err = ic(nil, &fakeStream{ctx: ctx}, &grpc.StreamServerInfo{FullMethod: "/pkg.Svc/Method"}, func(any, grpc.ServerStream) error {
		called = true
		return nil
	})

	require.False(t, called)
	// Client sees a generic message + correct code.
	st := status.Convert(err)
	require.Equal(t, codes.Unauthenticated, st.Code())
	require.Equal(t, "invalid credentials", st.Message())
	// Server log carries the detailed reason and the method...
	logged := buf.String()
	require.Contains(t, logged, "/pkg.Svc/Method")
	require.Contains(t, logged, "mismatch")
	// ...but never the token value.
	require.NotContains(t, logged, "s3cret")
	require.NotContains(t, logged, "wrong")
}
