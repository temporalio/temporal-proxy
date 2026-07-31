package api_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/temporalio/temporal-proxy/pkg/api"
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

			err := api.Reject(tt.code, tt.clientMsg, tt.detail)

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
