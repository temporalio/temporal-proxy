package auth_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/temporalio/temporal-proxy/internal/auth"
	"github.com/temporalio/temporal-proxy/internal/config"
)

func TestCredentialProviderFor(t *testing.T) {
	t.Parallel()

	t.Run("nil config yields nil provider", func(t *testing.T) {
		t.Parallel()
		cp, err := auth.CredentialProviderFor(nil)
		require.NoError(t, err)
		require.Nil(t, cp)
	})

	t.Run("static config yields a provider", func(t *testing.T) {
		t.Parallel()
		cp, err := auth.CredentialProviderFor(&config.CredentialConfig{
			Static: &config.StaticCredentialConfig{APIKey: "k"},
		})
		require.NoError(t, err)
		require.NotNil(t, cp)
		require.True(t, cp.RequireTransportSecurity())
	})

	t.Run("invalid static config errors", func(t *testing.T) {
		t.Parallel()
		cp, err := auth.CredentialProviderFor(&config.CredentialConfig{Static: &config.StaticCredentialConfig{}})
		require.Error(t, err)
		require.Nil(t, cp)
	})

	t.Run("present but empty block fails closed", func(t *testing.T) {
		t.Parallel()
		cp, err := auth.CredentialProviderFor(&config.CredentialConfig{})
		require.Error(t, err)
		require.Nil(t, cp)
	})

	t.Run("DialOptions returns the credential and strip interceptors", func(t *testing.T) {
		t.Parallel()
		cp, err := auth.CredentialProviderFor(&config.CredentialConfig{
			Static: &config.StaticCredentialConfig{APIKey: "k"},
		})
		require.NoError(t, err)
		require.Len(t, auth.DialOptions(cp), 3)
	})
}
