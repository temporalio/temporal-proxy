package kms

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"net/url"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/stretchr/testify/require"
	"go.uber.org/fx"
	"go.uber.org/fx/fxtest"

	"github.com/temporalio/temporal-proxy/internal/api"
	"github.com/temporalio/temporal-proxy/internal/config"
	"github.com/temporalio/temporal-proxy/internal/metrics"
	"github.com/temporalio/temporal-proxy/pkg/crypto"
	"github.com/temporalio/temporal-proxy/pkg/logger"
)

type refresherFunc func() error

func TestRunRotation_RefreshesImmediatelyThenEachInterval(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		var calls atomic.Int64
		r := refresherFunc(func() error {
			calls.Add(1)
			return nil
		})

		ctx, cancel := context.WithCancel(t.Context())
		go runRotation(ctx, r, time.Second, logger.NewNoopLogger())

		// The first refresh runs immediately; the goroutine then blocks on the
		// interval timer.
		synctest.Wait()
		require.Equal(t, int64(1), calls.Load())

		// Advance past three ticks (t=1s, 2s, 3s); the fourth at t=4s has not fired.
		time.Sleep(3500 * time.Millisecond)
		synctest.Wait()
		require.Equal(t, int64(4), calls.Load())

		// Cancellation stops the loop and no further refreshes occur.
		cancel()
		synctest.Wait()
		require.Equal(t, int64(4), calls.Load())
	})
}

func TestRunRotation_ContinuesAfterRefreshError(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		var calls atomic.Int64
		r := refresherFunc(func() error {
			calls.Add(1)
			return errors.New("kms unavailable")
		})

		ctx, cancel := context.WithCancel(t.Context())
		go runRotation(ctx, r, time.Second, logger.NewNoopLogger())

		synctest.Wait()
		require.Equal(t, int64(1), calls.Load())

		// A failing Refresh is logged and the loop keeps ticking.
		time.Sleep(2500 * time.Millisecond)
		synctest.Wait()
		require.Equal(t, int64(3), calls.Load())

		cancel()
		synctest.Wait()
	})
}

func TestCreateKEKs(t *testing.T) {
	t.Parallel()

	log := logger.NewNoopLogger()
	primary := distinctKeyURL(t, 1)

	t.Run("primary followed by decrypt uris, in order", func(t *testing.T) {
		t.Parallel()

		p := &config.KeyPolicy{URI: primary, DecryptURIs: []url.URL{distinctKeyURL(t, 2), distinctKeyURL(t, 3)}}
		keys, err := createKEKs(t.Context(), p, log, defaultNamespace, newFxTestReporter(t), nil)
		require.NoError(t, err)
		t.Cleanup(func() { closeKEKs(t, keys) })

		require.Len(t, keys, 3)
		require.Equal(t, "base64key://"+primary.Host, keys[0].ID())
	})

	t.Run("primary only", func(t *testing.T) {
		t.Parallel()

		keys, err := createKEKs(t.Context(), &config.KeyPolicy{URI: primary}, log, defaultNamespace, newFxTestReporter(t), nil)
		require.NoError(t, err)
		t.Cleanup(func() { closeKEKs(t, keys) })

		require.Len(t, keys, 1)
	})

	t.Run("bad primary uri errors", func(t *testing.T) {
		t.Parallel()

		_, err := createKEKs(t.Context(), &config.KeyPolicy{URI: url.URL{Scheme: "bogus", Host: "x"}}, log, defaultNamespace, newFxTestReporter(t), nil)
		require.Error(t, err)
	})

	t.Run("bad decrypt uri errors", func(t *testing.T) {
		t.Parallel()

		p := &config.KeyPolicy{URI: primary, DecryptURIs: []url.URL{{Scheme: "bogus", Host: "x"}}}
		_, err := createKEKs(t.Context(), p, log, defaultNamespace, newFxTestReporter(t), nil)
		require.Error(t, err)
	})
}

func TestKeyPolicyRegistryOpts(t *testing.T) {
	t.Parallel()

	log := logger.NewNoopLogger()

	// asDefault selects WithDefaultKey vs WithKeyForNamespace. A default key lets
	// NewKEKRegistry build; a namespace key alone does not (a default is
	// required), so the registry's success or failure reveals which option was
	// chosen.
	t.Run("asDefault registers a usable default key", func(t *testing.T) {
		t.Parallel()

		opts, err := keyPolicyRegistryOpts(t.Context(), &config.KeyPolicy{URI: distinctKeyURL(t, 1)}, log, defaultNamespace, true, newFxTestReporter(t), nil)
		require.NoError(t, err)

		reg, err := crypto.NewKEKRegistry(opts...)
		require.NoError(t, err)
		require.NoError(t, reg.Close())
	})

	t.Run("non-default registers only a namespace key", func(t *testing.T) {
		t.Parallel()

		opts, err := keyPolicyRegistryOpts(t.Context(), &config.KeyPolicy{URI: distinctKeyURL(t, 2)}, log, "other", false, newFxTestReporter(t), nil)
		require.NoError(t, err)

		_, err = crypto.NewKEKRegistry(opts...)
		require.Error(t, err)
	})

	// The decision is driven by asDefault, not by the namespace string: an
	// override for the literal "default" namespace must register a namespace key,
	// not silently overwrite the configured default key.
	t.Run("default namespace with asDefault false is a namespace key", func(t *testing.T) {
		t.Parallel()

		opts, err := keyPolicyRegistryOpts(t.Context(), &config.KeyPolicy{URI: distinctKeyURL(t, 3)}, log, defaultNamespace, false, newFxTestReporter(t), nil)
		require.NoError(t, err)

		_, err = crypto.NewKEKRegistry(opts...)
		require.Error(t, err)
	})
}

func TestCreateVault(t *testing.T) {
	t.Parallel()

	dk, err := newKEK(t.Context(), "testing://")
	require.NoError(t, err)
	reg, err := crypto.NewKEKRegistry(crypto.WithDefaultKey(dk))
	require.NoError(t, err)
	t.Cleanup(func() { _ = reg.Close() })

	tests := []struct {
		name string
		enc  config.Encryption
	}{
		{
			name: "with default policy",
			enc:  config.Encryption{CacheSize: 10, Default: &config.KeyPolicy{Duration: time.Hour, RenewBefore: time.Minute}},
		},
		{
			name: "without default policy",
			enc:  config.Encryption{CacheSize: 10},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			v, err := createVault(&config.Config{Encryption: tt.enc}, reg, newFxTestReporter(t))
			require.NoError(t, err)
			require.NotNil(t, v)
		})
	}
}

func TestCreateVault_AppliesOverrideKeyConfig(t *testing.T) {
	t.Parallel()

	dk, err := newKEK(t.Context(), "testing://")
	require.NoError(t, err)
	reg, err := crypto.NewKEKRegistry(crypto.WithDefaultKey(dk))
	require.NoError(t, err)
	t.Cleanup(func() { _ = reg.Close() })

	// An override with an invalid KeyConfig (RenewBefore >= Duration) must make
	// createVault fail. Config validation would normally reject this, but calling
	// createVault directly bypasses it, so the error proves the override's config
	// reaches WithKeyConfig instead of being dropped.
	cfg := &config.Config{Encryption: config.Encryption{
		CacheSize: 10,
		Default:   &config.KeyPolicy{Duration: time.Hour, RenewBefore: time.Minute},
		Overrides: map[string]config.KeyPolicy{
			"payments": {Duration: time.Hour, RenewBefore: time.Hour},
		},
	}}

	_, err = createVault(cfg, reg, newFxTestReporter(t))
	require.Error(t, err)
}

func TestCreateKEKRegistry(t *testing.T) {
	t.Parallel()

	lc := fxtest.NewLifecycle(t)
	cfg := &config.Config{Encryption: config.Encryption{Enabled: true, Default: &config.KeyPolicy{URI: distinctKeyURL(t, 1)}}}

	reg, err := createKEKRegistry(t.Context(), lc, cfg, logger.NewNoopLogger(), newFxTestReporter(t), nil)
	require.NoError(t, err)
	require.NotNil(t, reg)

	// The registered OnStop hook closes the registry without error.
	lc.RequireStart()
	lc.RequireStop()
}

func TestCreateKEKRegistry_OverrideKeySelectedByNamespace(t *testing.T) {
	t.Parallel()

	lc := fxtest.NewLifecycle(t)
	defaultURL := distinctKeyURL(t, 1)
	overrideURL := distinctKeyURL(t, 2)

	cfg := &config.Config{Encryption: config.Encryption{
		Enabled:   true,
		CacheSize: 10,
		Default:   &config.KeyPolicy{URI: defaultURL, Duration: time.Hour, RenewBefore: time.Minute},
		Overrides: map[string]config.KeyPolicy{
			"payments": {URI: overrideURL, Duration: time.Hour, RenewBefore: time.Minute},
		},
	}}

	reg, err := createKEKRegistry(t.Context(), lc, cfg, logger.NewNoopLogger(), newFxTestReporter(t), nil)
	require.NoError(t, err)
	lc.RequireStart()
	t.Cleanup(func() { lc.RequireStop() })

	vault, err := createVault(cfg, reg, newFxTestReporter(t))
	require.NoError(t, err)

	// The override namespace seals under its own KEK.
	msg, err := vault.Seal(t.Context(), "payments", []byte("secret"))
	require.NoError(t, err)
	require.Equal(t, "base64key://"+overrideURL.Host, msg.KeyMaterial.KEKID)

	// Any namespace without an override falls back to the default KEK.
	msg, err = vault.Seal(t.Context(), "other", []byte("secret"))
	require.NoError(t, err)
	require.Equal(t, "base64key://"+defaultURL.Host, msg.KeyMaterial.KEKID)
}

func TestModule_NoKeys_ProvidesNilVault(t *testing.T) {
	t.Parallel()

	// No key policy configured (and encryption disabled): there is nothing to
	// seal or open, so the vault is nil.
	var v *crypto.Vault
	app := fx.New(
		fx.Supply(fx.Annotate(t.Context(), fx.As(new(context.Context)))),
		fx.Supply(&config.Config{Encryption: config.Encryption{Enabled: false}}),
		fx.Provide(func() logger.Logger { return logger.NewNoopLogger() }),
		fx.Provide(func() *metrics.Factory { return metrics.New("test", promauto.With(prometheus.NewRegistry())) }),
		fx.Supply(api.Connections{}),
		Module,
		fx.Populate(&v),
		fx.NopLogger,
	)

	require.NoError(t, app.Err())
	require.Nil(t, v)
}

func TestModule_DisabledWithKeys_ProvidesVault(t *testing.T) {
	t.Parallel()

	// Encryption is off for new traffic but keys remain configured, so the vault
	// is still built to open payloads sealed earlier. The rotation goroutine is
	// gated on Enabled, so a clean start/stop confirms none was scheduled.
	var v *crypto.Vault
	cfg := &config.Config{Encryption: config.Encryption{
		Enabled:   false,
		CacheSize: 10,
		Default:   &config.KeyPolicy{URI: distinctKeyURL(t, 1), Duration: time.Hour, RenewBefore: time.Minute},
	}}

	app := fxtest.New(
		t,
		fx.Supply(fx.Annotate(t.Context(), fx.As(new(context.Context)))),
		fx.Supply(cfg),
		fx.Provide(func() logger.Logger { return logger.NewNoopLogger() }),
		fx.Provide(func() *metrics.Factory { return metrics.New("test", promauto.With(prometheus.NewRegistry())) }),
		fx.Supply(api.Connections{}),
		Module,
		fx.Populate(&v),
	)

	app.RequireStart()
	require.NotNil(t, v)
	app.RequireStop()
}

func TestModule_Enabled_ProvidesVaultAndRunsCleanly(t *testing.T) {
	t.Parallel()

	var v *crypto.Vault
	cfg := &config.Config{Encryption: config.Encryption{
		Enabled:   true,
		CacheSize: 10,
		Default:   &config.KeyPolicy{URI: distinctKeyURL(t, 1), Duration: time.Hour, RenewBefore: time.Minute},
	}}

	app := fxtest.New(
		t,
		fx.Supply(fx.Annotate(t.Context(), fx.As(new(context.Context)))),
		fx.Supply(cfg),
		fx.Provide(func() logger.Logger { return logger.NewNoopLogger() }),
		fx.Provide(func() *metrics.Factory { return metrics.New("test", promauto.With(prometheus.NewRegistry())) }),
		fx.Supply(api.Connections{}),
		Module,
		fx.Populate(&v),
	)

	app.RequireStart()
	require.NotNil(t, v)
	app.RequireStop()
}

func TestModule_Enabled_InvalidURI_FailsConstruction(t *testing.T) {
	t.Parallel()

	// An unopenable key URI must fail app construction, not surface later at
	// runtime. Module's Invoke depends on *crypto.Vault, so building the app
	// forces the erroring provider to run.
	cfg := &config.Config{Encryption: config.Encryption{
		Enabled: true,
		Default: &config.KeyPolicy{URI: url.URL{Scheme: "bogus", Host: "x"}},
	}}

	app := fx.New(
		fx.Supply(fx.Annotate(t.Context(), fx.As(new(context.Context)))),
		fx.Supply(cfg),
		fx.Provide(func() logger.Logger { return logger.NewNoopLogger() }),
		fx.Provide(func() *metrics.Factory { return metrics.New("test", promauto.With(prometheus.NewRegistry())) }),
		fx.Supply(api.Connections{}),
		Module,
		fx.NopLogger,
	)

	require.Error(t, app.Err())
}

func (f refresherFunc) Refresh() error { return f() }

// distinctKeyURL builds a testing:// key URI whose 32-byte key is filled with b,
// so different b values produce KEKs with distinct IDs. Bytes 1-3 base64-encode
// without URL-hostile characters.
func distinctKeyURL(t *testing.T, b byte) url.URL {
	t.Helper()

	key := base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{b}, 32))
	u, err := url.Parse("testing://" + key)
	require.NoError(t, err)

	return *u
}

func closeKEKs(t *testing.T, keys []crypto.KEK) {
	t.Helper()

	for _, k := range keys {
		require.NoError(t, k.Close())
	}
}

// newFxTestReporter builds a Reporter backed by its own private registry, so
// fx.go call sites under test have a Reporter to record into without
// colliding with any other test's metrics.
func newFxTestReporter(t *testing.T) *Reporter {
	t.Helper()
	return NewReporter(metrics.New("test", promauto.With(prometheus.NewRegistry())).ForSubsystem("encryption"))
}

func TestCreateKEKs_ResolvesExtensionURIs(t *testing.T) {
	t.Parallel()

	log := logger.NewNoopLogger()
	conns := extensionConns("audit")

	t.Run("mixes extension and keeper keys in one policy", func(t *testing.T) {
		t.Parallel()

		p := &config.KeyPolicy{
			URI:         *mustParse(t, "extension://audit/payments"),
			DecryptURIs: []url.URL{distinctKeyURL(t, 9), *mustParse(t, "extension://audit/legacy")},
		}

		keys, err := createKEKs(t.Context(), p, log, defaultNamespace, newFxTestReporter(t), conns)
		require.NoError(t, err)
		t.Cleanup(func() { closeKEKs(t, keys) })

		require.Len(t, keys, 3)
		require.Equal(t, "extension://audit/payments", keys[0].ID())
		require.Equal(t, "extension://audit/legacy", keys[2].ID())
	})

	t.Run("an unknown server aborts the policy", func(t *testing.T) {
		t.Parallel()

		p := &config.KeyPolicy{URI: *mustParse(t, "extension://missing/payments")}

		_, err := createKEKs(t.Context(), p, log, defaultNamespace, newFxTestReporter(t), conns)
		require.ErrorContains(t, err, `unknown extension server "missing"`)
	})

	t.Run("without connections an extension uri cannot resolve", func(t *testing.T) {
		t.Parallel()

		p := &config.KeyPolicy{URI: *mustParse(t, "extension://audit/payments")}

		_, err := createKEKs(t.Context(), p, log, defaultNamespace, newFxTestReporter(t), nil)
		require.ErrorContains(t, err, `unknown extension server "audit"`)
	})
}
