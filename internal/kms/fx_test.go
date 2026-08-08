package kms_test

import (
	"context"
	"net/url"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/stretchr/testify/require"
	"go.uber.org/fx"
	"go.uber.org/fx/fxtest"

	"github.com/temporalio/temporal-proxy/internal/api"
	"github.com/temporalio/temporal-proxy/internal/config"
	"github.com/temporalio/temporal-proxy/internal/kms"
	"github.com/temporalio/temporal-proxy/internal/metrics"
	"github.com/temporalio/temporal-proxy/pkg/crypto"
	"github.com/temporalio/temporal-proxy/pkg/logger"
)

func TestModule_NoKeys_ProvidesNilVault(t *testing.T) {
	t.Parallel()

	// No key policy configured (and encryption disabled): there is nothing to
	// seal or open, so the vault is nil.
	v, err := buildVault(t, &config.Config{Encryption: config.Encryption{Enabled: false}}, logger.NewNoopLogger(), nil)
	require.NoError(t, err)
	require.Nil(t, v)
}

func TestModule_DisabledWithKeys_ProvidesVault(t *testing.T) {
	t.Parallel()

	// Encryption is off for new traffic but keys remain configured, so the vault
	// is still built to open payloads sealed earlier. The rotation goroutine is
	// gated on Enabled, so a clean start/stop confirms none was scheduled.
	v := startVault(t, encryptionConfig(false, keyPolicy(t, 1)))
	require.NotNil(t, v)
}

func TestModule_Enabled_ProvidesVaultAndRunsCleanly(t *testing.T) {
	t.Parallel()

	// A clean stop also proves the registry's OnStop hook closed every KEK without
	// error, since fxtest fails the test on a stop error.
	v := startVault(t, encryptionConfig(true, keyPolicy(t, 1)))
	require.NotNil(t, v)
}

func TestModule_EnabledWithoutKeys_DoesNotScheduleRotation(t *testing.T) {
	t.Parallel()

	// Encryption enabled with no key policy yields a nil vault. Rotation must
	// not be scheduled against it.
	cfg := &config.Config{Encryption: config.Encryption{Enabled: true}}

	var v *crypto.Vault
	app := fx.New(append(
		moduleOptions(t, cfg, logger.NewNoopLogger(), api.Connections{}),
		fx.Populate(&v),
		fx.NopLogger,
	)...)

	require.NoError(t, app.Err())
	require.Nil(t, v)

	startCtx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	require.NoError(t, app.Start(startCtx))

	stopCtx, stop := context.WithTimeout(context.WithoutCancel(t.Context()), 5*time.Second)
	defer stop()
	require.NoError(t, app.Stop(stopCtx))
}

func TestModule_Enabled_InvalidURI_FailsConstruction(t *testing.T) {
	t.Parallel()

	// An unopenable key URI must fail app construction, not surface later at
	// runtime. Module's Invoke depends on *crypto.Vault, so building the app
	// forces the erroring provider to run.
	cfg := encryptionConfig(true, config.KeyPolicy{URI: url.URL{Scheme: "bogus", Host: "x"}})

	_, err := buildVault(t, cfg, logger.NewNoopLogger(), nil)
	require.Error(t, err)
}

func TestModule_InvalidDecryptURI_FailsConstruction(t *testing.T) {
	t.Parallel()

	// A decrypt-only URI is opened alongside the primary, so a bad one has to fail
	// construction too rather than leaving a key that cannot open old payloads.
	policy := keyPolicy(t, 1)
	policy.DecryptURIs = []url.URL{{Scheme: "bogus", Host: "x"}}

	_, err := buildVault(t, encryptionConfig(true, policy), logger.NewNoopLogger(), nil)
	require.Error(t, err)
}

func TestModule_InvalidOverrideKeyConfig_FailsConstruction(t *testing.T) {
	t.Parallel()

	// An override with an invalid KeyConfig (RenewBefore >= Duration) must fail
	// construction. Config validation would normally reject it, but the module is
	// handed a Config directly, so the error proves the override's own duration
	// reaches the vault rather than being dropped in favour of the default's.
	cfg := encryptionConfig(true, keyPolicy(t, 1))
	cfg.Encryption.Overrides = map[string]config.KeyPolicy{
		"payments": {URI: testingKeyURL(t, 2), Duration: time.Hour, RenewBefore: time.Hour},
	}

	_, err := buildVault(t, cfg, logger.NewNoopLogger(), nil)
	require.ErrorContains(t, err, `key config for "payments"`)
}

func TestModule_SelectsKeyByNamespace(t *testing.T) {
	t.Parallel()

	defaultURL, overrideURL := testingKeyURL(t, 1), testingKeyURL(t, 2)

	cfg := encryptionConfig(true, keyPolicy(t, 1))
	cfg.Encryption.Overrides = map[string]config.KeyPolicy{
		"payments": {URI: overrideURL, Duration: time.Hour, RenewBefore: time.Minute},
	}

	v := startVault(t, cfg)

	// The override namespace seals under its own KEK.
	msg, err := v.Seal(t.Context(), "payments", []byte("secret"))
	require.NoError(t, err)
	require.Equal(t, "base64key://"+overrideURL.Host, msg.KeyMaterial.KEKID)

	// Any namespace without an override falls back to the default KEK.
	msg, err = v.Seal(t.Context(), "other", []byte("secret"))
	require.NoError(t, err)
	require.Equal(t, "base64key://"+defaultURL.Host, msg.KeyMaterial.KEKID)
}

func TestModule_OverrideForDefaultNamespaceKeepsTheDefaultKey(t *testing.T) {
	t.Parallel()

	// An override for the literal "default" namespace must register a namespace
	// key, not overwrite the configured default key: namespaces with no override of
	// their own still have to reach the default.
	defaultURL, overrideURL := testingKeyURL(t, 1), testingKeyURL(t, 2)

	cfg := encryptionConfig(true, keyPolicy(t, 1))
	cfg.Encryption.Overrides = map[string]config.KeyPolicy{
		"default": {URI: overrideURL, Duration: time.Hour, RenewBefore: time.Minute},
	}

	v := startVault(t, cfg)

	msg, err := v.Seal(t.Context(), "default", []byte("secret"))
	require.NoError(t, err)
	require.Equal(t, "base64key://"+overrideURL.Host, msg.KeyMaterial.KEKID)

	msg, err = v.Seal(t.Context(), "other", []byte("secret"))
	require.NoError(t, err)
	require.Equal(t, "base64key://"+defaultURL.Host, msg.KeyMaterial.KEKID)
}

func TestModule_OpensPayloadsSealedByADecryptOnlyKey(t *testing.T) {
	t.Parallel()

	// A rotated-out key lives on in DecryptURIs. Sealing under it while it is
	// primary and then opening that payload from a second app where it is only a
	// decrypt URI is the whole point of the setting, and the only way to observe
	// that those keys reach the registry.
	rotatedOut := testingKeyURL(t, 7)

	sealed, err := startVault(t, encryptionConfig(true, config.KeyPolicy{
		URI: rotatedOut, Duration: time.Hour, RenewBefore: time.Minute,
	})).Seal(t.Context(), "ns1", []byte("secret"))
	require.NoError(t, err)
	require.Equal(t, "base64key://"+rotatedOut.Host, sealed.KeyMaterial.KEKID)

	current := keyPolicy(t, 1)
	current.DecryptURIs = []url.URL{rotatedOut}

	pt, err := startVault(t, encryptionConfig(true, current)).Open(t.Context(), sealed)
	require.NoError(t, err)
	require.Equal(t, []byte("secret"), pt)
}

func TestModule_UnknownExtensionServer_FailsConstruction(t *testing.T) {
	t.Parallel()

	cfg := encryptionConfig(true, config.KeyPolicy{
		URI: *mustParse(t, "extension://missing/payments"), Duration: time.Hour, RenewBefore: time.Minute,
	})

	_, err := buildVault(t, cfg, logger.NewNoopLogger(), extensionConns("audit"))
	require.ErrorContains(t, err, `unknown extension server "missing"`)
}

func TestModule_MixesExtensionAndKeeperKeysInOnePolicy(t *testing.T) {
	t.Parallel()

	// Both kinds of key may appear in one policy. Nothing is sealed here: these
	// connections fail any call, so construction succeeding is what shows every URI
	// resolved.
	cfg := encryptionConfig(true, config.KeyPolicy{
		URI:         *mustParse(t, "extension://audit/payments"),
		DecryptURIs: []url.URL{testingKeyURL(t, 9), *mustParse(t, "extension://audit/legacy")},
		Duration:    time.Hour,
		RenewBefore: time.Minute,
	})

	v, err := buildVault(t, cfg, logger.NewNoopLogger(), extensionConns("audit"))
	require.NoError(t, err)
	require.NotNil(t, v)
}

// moduleOptions are the dependencies Module needs, wired for a test: a private
// metrics registry so collectors cannot collide with another test's, the given
// logger, and conns for any extension key URIs.
func moduleOptions(t *testing.T, cfg *config.Config, log logger.Logger, conns api.Connections) []fx.Option {
	t.Helper()

	return []fx.Option{
		fx.Supply(fx.Annotate(t.Context(), fx.As(new(context.Context)))),
		fx.Supply(cfg),
		fx.Provide(func() logger.Logger { return log }),
		fx.Provide(func() *metrics.Factory {
			return metrics.New("test", promauto.With(prometheus.NewRegistry()))
		}),
		fx.Supply(conns),
		kms.Module,
	}
}

// buildVault constructs an app around Module and returns the vault it provides
// together with any construction error, without starting the app.
func buildVault(
	t *testing.T,
	cfg *config.Config,
	log logger.Logger,
	conns api.Connections,
) (*crypto.Vault, error) {
	t.Helper()

	if conns == nil {
		conns = api.Connections{}
	}

	var v *crypto.Vault
	app := fx.New(append(
		moduleOptions(t, cfg, log, conns),
		fx.Populate(&v),
		fx.NopLogger,
	)...)

	return v, app.Err()
}

// startVault builds and starts an app around Module, stopping it when the test
// ends. Construction, start, or stop failing fails the test.
func startVault(t *testing.T, cfg *config.Config) *crypto.Vault {
	t.Helper()

	var v *crypto.Vault
	app := fxtest.New(t, append(
		moduleOptions(t, cfg, logger.NewNoopLogger(), api.Connections{}),
		fx.Populate(&v),
	)...)

	app.RequireStart()
	t.Cleanup(func() { app.RequireStop() })

	return v
}

// encryptionConfig builds a Config whose encryption is governed by policy.
func encryptionConfig(enabled bool, policy config.KeyPolicy) *config.Config {
	return &config.Config{Encryption: config.Encryption{
		Enabled:   enabled,
		CacheSize: 10,
		Default:   &policy,
	}}
}

// keyPolicy builds a usable default policy around the testing key filled with b.
func keyPolicy(t *testing.T, b byte) config.KeyPolicy {
	t.Helper()

	return config.KeyPolicy{
		URI:         testingKeyURL(t, b),
		Duration:    time.Hour,
		RenewBefore: time.Minute,
	}
}

func mustParse(t *testing.T, raw string) *url.URL {
	t.Helper()

	u, err := url.Parse(raw)
	require.NoError(t, err)

	return u
}
