package rpc_test

import (
	"context"
	"errors"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/temporalio/temporal-proxy/internal/rpc"
)

func TestReject(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		code      codes.Code
		clientMsg string
		detail    string
	}{
		{
			name:      "invalid credential",
			code:      codes.Unauthenticated,
			clientMsg: "invalid credentials",
			detail:    "static token: value mismatch",
		},
		{
			name:      "authenticator unhealthy",
			code:      codes.Unavailable,
			clientMsg: "authentication temporarily unavailable",
			detail:    "jwks: fetch failed: connection refused",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := rpc.Reject(tt.code, tt.clientMsg, tt.detail)

			// Error is what a logger records, so it must carry the detail.
			require.EqualError(t, err, tt.detail)

			// gRPC sends GRPCStatus, so the caller gets the code and the generic
			// message and never learns why the credential was refused.
			st, ok := status.FromError(err)
			require.True(t, ok)
			require.Equal(t, tt.code, st.Code())
			require.Equal(t, tt.clientMsg, st.Message())
			require.NotContains(t, st.Message(), tt.detail)
		})
	}
}

func TestStatusError(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		err      error
		code     codes.Code
		msg      string
		verbatim bool
	}{
		{name: "nil stays nil", code: codes.OK},
		{
			name:     "an error already carrying a status is forwarded verbatim",
			err:      status.Error(codes.NotFound, "namespace not found"),
			code:     codes.NotFound,
			msg:      "namespace not found",
			verbatim: true,
		},
		{
			name: "a raw cancellation maps to its status",
			err:  context.Canceled,
			code: codes.Canceled,
		},
		{
			name: "a raw deadline maps to its status",
			err:  context.DeadlineExceeded,
			code: codes.DeadlineExceeded,
		},
		{
			// io.EOF reaches here when a caller half-closes mid-request and carries no
			// status of its own, so it must not surface as an opaque Unknown.
			name: "io.EOF is reported as Internal naming the step",
			err:  io.EOF,
			code: codes.Internal,
			msg:  "proxy: sending the response failed: EOF",
		},
		{
			name: "anything else is reported as Internal naming the step",
			err:  errors.New("boom"),
			code: codes.Internal,
			msg:  "proxy: sending the response failed: boom",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// The caller owns the whole message, prefix included, as the forwarder does.
			got := rpc.StatusError("proxy: sending the response failed", tt.err)
			require.Equal(t, tt.code, status.Code(got))

			// An error that already carries a status is the same error, not a copy of
			// it, so a caller can still match on it.
			if tt.verbatim {
				require.ErrorIs(t, got, tt.err)
			}

			if tt.msg == "" {
				return
			}

			require.ErrorContains(t, got, tt.msg)
		})
	}
}
