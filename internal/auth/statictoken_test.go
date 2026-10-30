package auth_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"go.temporal.io/proxy/internal/auth"
	"go.temporal.io/proxy/internal/transport/meta"
)

func TestStaticTokenAuthenticator(t *testing.T) {
	t.Parallel()

	t.Run("empty token is rejected at construction", func(t *testing.T) {
		t.Parallel()
		_, err := auth.NewStaticTokenAuthenticator("", "", "", false)
		require.Error(t, err)
	})

	a, err := auth.NewStaticTokenAuthenticator("s3cret", "", "", false)
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
			err := a.Authenticate(t.Context(), meta.Target{}, tt.md)
			require.Equal(t, tt.code, status.Code(err))
		})
	}
}

func TestStaticTokenAuthenticatorNoScheme(t *testing.T) {
	t.Parallel()

	a, err := auth.NewStaticTokenAuthenticator("s3cret", "x-token", "", true)
	require.NoError(t, err)

	tests := []struct {
		name string
		md   metadata.MD
		code codes.Code
	}{
		{"bare token", metadata.Pairs("x-token", "s3cret"), codes.OK},
		{"prefix is not stripped", metadata.Pairs("x-token", "Bearer s3cret"), codes.Unauthenticated},
		{"wrong token", metadata.Pairs("x-token", "nope"), codes.Unauthenticated},
		{"missing header", metadata.MD{}, codes.Unauthenticated},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			err := a.Authenticate(t.Context(), meta.Target{}, tt.md)
			require.Equal(t, tt.code, status.Code(err))
		})
	}
}
