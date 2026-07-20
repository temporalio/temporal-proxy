package auth_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/temporalio/temporal-proxy/internal/auth"
)

func TestStaticCredentialProvider(t *testing.T) {
	t.Parallel()

	t.Run("empty api key is rejected", func(t *testing.T) {
		t.Parallel()
		_, err := auth.NewStaticCredentialProvider("", "", "")
		require.Error(t, err)
	})

	t.Run("defaults produce a bearer authorization header", func(t *testing.T) {
		t.Parallel()
		p, err := auth.NewStaticCredentialProvider("k3y", "", "")
		require.NoError(t, err)

		md, err := p.GetRequestMetadata(t.Context())
		require.NoError(t, err)
		require.Equal(t, map[string]string{"authorization": "Bearer k3y"}, md)
		require.True(t, p.RequireTransportSecurity())
	})

	t.Run("custom header and scheme", func(t *testing.T) {
		t.Parallel()
		p, err := auth.NewStaticCredentialProvider("k3y", "x-api-key", "Token")
		require.NoError(t, err)

		md, err := p.GetRequestMetadata(t.Context())
		require.NoError(t, err)
		require.Equal(t, map[string]string{"x-api-key": "Token k3y"}, md)
	})

	t.Run("mixed-case header is lowercased to a canonical metadata key", func(t *testing.T) {
		t.Parallel()
		p, err := auth.NewStaticCredentialProvider("k3y", "Authorization", "")
		require.NoError(t, err)

		require.Equal(t, "authorization", p.Header())

		md, err := p.GetRequestMetadata(t.Context())
		require.NoError(t, err)
		require.Equal(t, map[string]string{"authorization": "Bearer k3y"}, md)
	})
}
