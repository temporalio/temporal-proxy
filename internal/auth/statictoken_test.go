package auth_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/temporalio/temporal-proxy/internal/auth"
)

func TestStaticTokenAuthenticator(t *testing.T) {
	t.Parallel()

	t.Run("empty token is rejected at construction", func(t *testing.T) {
		t.Parallel()
		_, err := auth.NewStaticTokenAuthenticator("", "", "")
		require.Error(t, err)
	})

	a, err := auth.NewStaticTokenAuthenticator("s3cret", "", "")
	require.NoError(t, err)

	tests := []struct {
		name string
		md   metadata.MD
		code codes.Code
	}{
		{"valid", metadata.Pairs("authorization", "Bearer s3cret"), codes.OK},
		{"scheme case-insensitive", metadata.Pairs("authorization", "bearer s3cret"), codes.OK},
		{"wrong token", metadata.Pairs("authorization", "Bearer nope"), codes.Unauthenticated},
		{"missing header", metadata.MD{}, codes.Unauthenticated},
		{"missing scheme", metadata.Pairs("authorization", "s3cret"), codes.Unauthenticated},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			err := a.Authenticate(t.Context(), tt.md)
			require.Equal(t, tt.code, status.Code(err))
		})
	}
}
