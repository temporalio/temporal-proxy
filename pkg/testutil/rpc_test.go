package testutil_test

import (
	"errors"
	"io"
	"testing"
	"testing/synctest"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	"github.com/temporalio/temporal-proxy/pkg/testutil"
)

func TestClientConnInvoke(t *testing.T) {
	t.Parallel()

	t.Run("fills the header and trailer the caller asked for", func(t *testing.T) {
		t.Parallel()

		cc := &testutil.ClientConn{
			Header:  metadata.Pairs("x-header", "h"),
			Trailer: metadata.Pairs("x-trailer", "t"),
		}

		var header, trailer metadata.MD
		err := cc.Invoke(t.Context(), "/pkg.Svc/M", nil, nil, grpc.Header(&header), grpc.Trailer(&trailer))
		require.NoError(t, err)
		require.Equal(t, []string{"h"}, header.Get("x-header"))
		require.Equal(t, []string{"t"}, trailer.Get("x-trailer"))
	})

	t.Run("ignores call options it does not fill", func(t *testing.T) {
		t.Parallel()

		cc := &testutil.ClientConn{}
		require.NoError(t, cc.Invoke(t.Context(), "/pkg.Svc/M", nil, nil, grpc.WaitForReady(true)))
	})

	t.Run("reports the configured error", func(t *testing.T) {
		t.Parallel()

		want := errors.New("upstream refused")
		cc := &testutil.ClientConn{InvokeErr: want}
		require.ErrorIs(t, cc.Invoke(t.Context(), "/pkg.Svc/M", nil, nil), want)
	})
}

func TestClientConnNewStream(t *testing.T) {
	t.Parallel()

	t.Run("returns the configured stream", func(t *testing.T) {
		t.Parallel()

		want := &testutil.ClientStream{}
		cc := &testutil.ClientConn{Stream: want}

		got, err := cc.NewStream(t.Context(), &grpc.StreamDesc{}, "/pkg.Svc/M")
		require.NoError(t, err)
		require.Same(t, want, got)
	})

	t.Run("reports the configured error instead of a stream", func(t *testing.T) {
		t.Parallel()

		want := errors.New("cannot open")
		cc := &testutil.ClientConn{StreamErr: want, Stream: &testutil.ClientStream{}}

		got, err := cc.NewStream(t.Context(), &grpc.StreamDesc{}, "/pkg.Svc/M")
		require.ErrorIs(t, err, want)
		require.Nil(t, got)
	})
}

func TestClientStream(t *testing.T) {
	t.Parallel()

	t.Run("reports the configured errors", func(t *testing.T) {
		t.Parallel()

		headerErr := errors.New("header")
		sendErr := errors.New("send")
		recvErr := errors.New("recv")
		cs := &testutil.ClientStream{HeaderErr: headerErr, SendErr: sendErr, RecvErr: recvErr}

		md, err := cs.Header()
		require.Nil(t, md)
		require.ErrorIs(t, err, headerErr)
		require.ErrorIs(t, cs.SendMsg(nil), sendErr)
		require.ErrorIs(t, cs.RecvMsg(nil), recvErr)
	})

	t.Run("the zero value succeeds and carries no metadata", func(t *testing.T) {
		t.Parallel()

		cs := &testutil.ClientStream{}

		md, err := cs.Header()
		require.NoError(t, err)
		require.Nil(t, md)
		require.Nil(t, cs.Trailer())
		require.NoError(t, cs.CloseSend())
		require.NoError(t, cs.SendMsg(nil))
		require.NoError(t, cs.RecvMsg(nil))
		require.NotNil(t, cs.Context())
	})
}

func TestClientStreamBlockRecv(t *testing.T) {
	t.Parallel()

	// The blocking behaviour is the point of BlockRecv: a caller uses it to park
	// one reader so a concurrent one reports first. Assert it really parks, rather
	// than trusting a sleep to have been long enough.
	synctest.Test(t, func(t *testing.T) {
		release := make(chan struct{})
		cs := &testutil.ClientStream{BlockRecv: release, RecvErr: errors.New("never returned")}

		got := make(chan error, 1)
		go func() { got <- cs.RecvMsg(nil) }()

		// Wait for every goroutine in the bubble to block; RecvMsg is still parked.
		synctest.Wait()
		require.Empty(t, got, "expected RecvMsg to block until BlockRecv is closed")

		// Releasing reports a clean end of stream, not the configured RecvErr.
		close(release)
		synctest.Wait()
		require.ErrorIs(t, <-got, io.EOF)
	})
}

func TestServerStream(t *testing.T) {
	t.Parallel()

	t.Run("reports the configured errors", func(t *testing.T) {
		t.Parallel()

		headerErr := errors.New("header")
		sendErr := errors.New("send")
		recvErr := errors.New("recv")
		ss := testutil.ServerStream{HeaderErr: headerErr, SendErr: sendErr, RecvErr: recvErr}

		require.ErrorIs(t, ss.SetHeader(nil), headerErr)
		require.ErrorIs(t, ss.SendHeader(nil), headerErr)
		require.ErrorIs(t, ss.SendMsg(nil), sendErr)
		require.ErrorIs(t, ss.RecvMsg(nil), recvErr)
	})

	t.Run("carries the configured context and discards trailers", func(t *testing.T) {
		t.Parallel()

		ctx := t.Context()
		ss := testutil.ServerStream{Ctx: ctx}

		require.Equal(t, ctx, ss.Context())
		require.NoError(t, ss.SetHeader(nil))
		require.NoError(t, ss.SendHeader(nil))
		require.NoError(t, ss.SendMsg(nil))
		require.NoError(t, ss.RecvMsg(nil))

		ss.SetTrailer(metadata.Pairs("x-trailer", "t"))
	})
}

func TestServerTransportStream(t *testing.T) {
	t.Parallel()

	sts := testutil.ServerTransportStream{FullMethodName: "/pkg.Svc/M"}

	require.Equal(t, "/pkg.Svc/M", sts.Method())
	require.NoError(t, sts.SetHeader(nil))
	require.NoError(t, sts.SendHeader(nil))
	require.NoError(t, sts.SetTrailer(nil))

	// It satisfies the interface gRPC installs into a request context.
	ctx := grpc.NewContextWithServerTransportStream(t.Context(), sts)
	require.Equal(t, "/pkg.Svc/M", grpc.ServerTransportStreamFromContext(ctx).Method())
}
