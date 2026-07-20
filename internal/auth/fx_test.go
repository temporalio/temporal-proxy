package auth_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/fx"

	"github.com/temporalio/temporal-proxy/internal/auth"
	"github.com/temporalio/temporal-proxy/internal/config"
)

func TestModuleProvidesAuthenticator(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		cfg           *config.Config
		admitsMissing bool // the default authenticator admits requests with no credentials
	}{
		{"no auth configured uses default authenticator", &config.Config{}, true},
		{"static token configured", &config.Config{Auth: &config.AuthConfig{StaticToken: &config.StaticTokenConfig{Token: "x"}}}, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var got auth.Authenticator
			app := fx.New(
				fx.Supply(tt.cfg),
				auth.Module,
				fx.Populate(&got),
				fx.NopLogger,
			)
			require.NoError(t, app.Err())
			require.NotNil(t, got)

			if tt.admitsMissing {
				require.NoError(t, got.Authenticate(t.Context(), nil))
			}
		})
	}
}

func TestModuleInvalidConfigFailsApp(t *testing.T) {
	t.Parallel()

	cfg := &config.Config{Auth: &config.AuthConfig{JWKS: &config.JWKSConfig{URL: "not-a-url"}}}

	var got auth.Authenticator
	app := fx.New(
		fx.Supply(cfg),
		auth.Module,
		fx.Populate(&got),
		fx.NopLogger,
	)
	require.Error(t, app.Err())
}

func TestModuleEmptyAuthBlockFailsApp(t *testing.T) {
	t.Parallel()

	cfg := &config.Config{Auth: &config.AuthConfig{}} // present but neither selected

	var got auth.Authenticator
	app := fx.New(
		fx.Supply(cfg),
		auth.Module,
		fx.Populate(&got),
		fx.NopLogger,
	)
	require.Error(t, app.Err())
}
