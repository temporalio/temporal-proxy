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
		moduleOptions(t, cfg, logger.NewNoopLogger(), api.Connections{}, prometheus.NewRegistry()),
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

// moduleOptions are the dependencies Module needs, wired for a test: metrics on
// reg, the given logger, and conns for any extension key URIs. Callers pass a
// registry of their own so collectors cannot collide with another test's, and so
// a test that cares can read what the vault reported.
func moduleOptions(
	t *testing.T,
	cfg *config.Config,
	log logger.Logger,
	conns api.Connections,
	reg *prometheus.Registry,
) []fx.Option {
	t.Helper()

	return []fx.Option{
		fx.Supply(fx.Annotate(t.Context(), fx.As(new(context.Context)))),
		fx.Supply(cfg),
		fx.Provide(func() logger.Logger { return log }),
		fx.Provide(func() *metrics.Factory {
			return metrics.New("test", promauto.With(reg))
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
		moduleOptions(t, cfg, log, conns, prometheus.NewRegistry()),
		fx.Populate(&v),
		fx.NopLogger,
	)...)

	return v, app.Err()
}

// startVault builds and starts an app around Module, stopping it when the test
// ends. Construction, start, or stop failing fails the test.
func startVault(t *testing.T, cfg *config.Config) *crypto.Vault {
	t.Helper()

	return startVaultWithRegistry(t, cfg, prometheus.NewRegistry())
}

// startVaultWithRegistry is [startVault] with the metrics registry in the
// caller's hands, for a test that asserts on what the vault reported rather than
// on what it returned.
func startVaultWithRegistry(t *testing.T, cfg *config.Config, reg *prometheus.Registry) *crypto.Vault {
	t.Helper()

	var v *crypto.Vault
	app := fxtest.New(t, append(
		moduleOptions(t, cfg, logger.NewNoopLogger(), api.Connections{}, reg),
		fx.Populate(&v),
	)...)

	app.RequireStart()
	t.Cleanup(func() { app.RequireStop() })

	return v
}

// TestModule_OmittedCacheSizeStillCachesDEKs guards the DEK cache against the
// config layer switching it off by saying nothing. crypto.NewVault defaults the
// cache to crypto.DefaultCacheSize, but createVault passes the configured size
// unconditionally, so an absent cacheSize used to arrive as zero - which
// WithCacheSize documents as "disable" - and every payload opened cost a KEK
// unwrap. Most configs omit the field, examples/kms/config.yaml among them.
func TestModule_OmittedCacheSizeStillCachesDEKs(t *testing.T) {
	t.Parallel()

	cfg := encryptionConfig(true, keyPolicy(t, 1))
	cfg.Encryption.CacheSize = nil

	reg := prometheus.NewRegistry()
	v := startVaultWithRegistry(t, cfg, reg)

	msg, err := v.Seal(t.Context(), "ns", []byte("secret"))
	require.NoError(t, err)

	// The first Open populates the cache and the second must be served from it.
	for range 2 {
		got, err := v.Open(t.Context(), msg)
		require.NoError(t, err)
		require.Equal(t, []byte("secret"), got)
	}

	hits := gather(t, reg, "test_encryption_dek_cache_hits_total")
	require.NotNil(t, hits, "a disabled cache reports no hits at all")
	require.Equal(t, 1.0, hits.GetMetric()[0].GetCounter().GetValue(), "the second open must come from the cache")
}

// TestModule_ZeroCacheSizeDisablesTheCache is the other half: zero remains a way
// to turn the cache off, now that it has to be written down rather than implied
// by an absent field.
func TestModule_ZeroCacheSizeDisablesTheCache(t *testing.T) {
	t.Parallel()

	cfg := encryptionConfig(true, keyPolicy(t, 1))
	cfg.Encryption.CacheSize = new(0)

	reg := prometheus.NewRegistry()
	v := startVaultWithRegistry(t, cfg, reg)

	msg, err := v.Seal(t.Context(), "ns", []byte("secret"))
	require.NoError(t, err)

	for range 2 {
		_, err := v.Open(t.Context(), msg)
		require.NoError(t, err)
	}

	// The counters are created eagerly, so they exist at zero whether the cache is
	// off or merely idle - which is exactly why a disabled cache is logged at
	// startup. Assert the value, not the family.
	hits := gather(t, reg, "test_encryption_dek_cache_hits_total")
	require.NotNil(t, hits)
	require.Zero(t, hits.GetMetric()[0].GetCounter().GetValue(), "there is no cache to hit")
}

// encryptionConfig builds a Config whose encryption is governed by policy.
func encryptionConfig(enabled bool, policy config.KeyPolicy) *config.Config {
	return &config.Config{Encryption: config.Encryption{
		Enabled:   enabled,
		CacheSize: new(10),
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
