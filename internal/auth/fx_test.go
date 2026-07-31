package auth_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/fx"
	"google.golang.org/grpc"

	"github.com/temporalio/temporal-proxy/internal/api"
	"github.com/temporalio/temporal-proxy/internal/auth"
	"github.com/temporalio/temporal-proxy/internal/config"
)

// nopConn stands in for a connection to an extension server. The module only has
// to find one by name and hand it to the client, so nothing is dialed here.
type nopConn struct{ grpc.ClientConnInterface }

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
				fx.Supply(api.Connections{}),
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

func TestModuleProvidesExternalAuthenticator(t *testing.T) {
	t.Parallel()

	cfg := &config.Config{Auth: &config.AuthConfig{External: &config.ExternalAuthConfig{
		Name:              "policy",
		CredentialHeaders: []string{"authorization"},
	}}}

	var got auth.Authenticator
	app := fx.New(
		fx.Supply(cfg),
		fx.Supply(api.Connections{"policy": nopConn{}}),
		auth.Module,
		fx.Populate(&got),
		fx.NopLogger,
	)
	require.NoError(t, app.Err())

	// The declared headers have to reach the client, since they are what it lifts
	// into the request and what the interceptor strips before proxying upstream.
	require.Equal(t, []string{"authorization"}, got.SecureHeaders())
}

func TestModuleExternalNamingUnknownServerFailsApp(t *testing.T) {
	t.Parallel()

	// Fail at construction rather than on the first request: an authenticator that
	// cannot reach its provider would otherwise reject every caller at runtime.
	cfg := &config.Config{Auth: &config.AuthConfig{External: &config.ExternalAuthConfig{Name: "absent"}}}

	var got auth.Authenticator
	app := fx.New(
		fx.Supply(cfg),
		fx.Supply(api.Connections{"policy": nopConn{}}),
		auth.Module,
		fx.Populate(&got),
		fx.NopLogger,
	)
	require.ErrorContains(t, app.Err(), "absent")
}

func TestModuleInvalidConfigFailsApp(t *testing.T) {
	t.Parallel()

	cfg := &config.Config{Auth: &config.AuthConfig{JWKS: &config.JWKSConfig{URL: "not-a-url"}}}

	var got auth.Authenticator
	app := fx.New(
		fx.Supply(cfg),
		fx.Supply(api.Connections{}),
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
		fx.Supply(api.Connections{}),
		auth.Module,
		fx.Populate(&got),
		fx.NopLogger,
	)
	require.Error(t, app.Err())
}
