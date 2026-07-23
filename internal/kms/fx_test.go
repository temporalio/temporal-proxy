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

	"github.com/stretchr/testify/require"
	"go.uber.org/fx"
	"go.uber.org/fx/fxtest"

	"github.com/temporalio/temporal-proxy/internal/config"
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
		keys, err := createKEKs(t.Context(), p, log, defaultNamespace)
		require.NoError(t, err)
		t.Cleanup(func() { closeKEKs(t, keys) })

		require.Len(t, keys, 3)
		require.Equal(t, "base64key://"+primary.Host, keys[0].ID())
	})

	t.Run("primary only", func(t *testing.T) {
		t.Parallel()

		keys, err := createKEKs(t.Context(), &config.KeyPolicy{URI: primary}, log, defaultNamespace)
		require.NoError(t, err)
		t.Cleanup(func() { closeKEKs(t, keys) })

		require.Len(t, keys, 1)
	})

	t.Run("bad primary uri errors", func(t *testing.T) {
		t.Parallel()

		_, err := createKEKs(t.Context(), &config.KeyPolicy{URI: url.URL{Scheme: "bogus", Host: "x"}}, log, defaultNamespace)
		require.Error(t, err)
	})

	t.Run("bad decrypt uri errors", func(t *testing.T) {
		t.Parallel()

		p := &config.KeyPolicy{URI: primary, DecryptURIs: []url.URL{{Scheme: "bogus", Host: "x"}}}
		_, err := createKEKs(t.Context(), p, log, defaultNamespace)
		require.Error(t, err)
	})
}

func TestKeyPolicyRegistryOpts(t *testing.T) {
	t.Parallel()

	log := logger.NewNoopLogger()

	// The default namespace registers a default key, so the registry builds.
	// A non-default namespace registers only a namespace key, which is not
	// sufficient on its own (NewKEKRegistry requires a default), proving the
	// branch chose WithKeyForNamespace over WithDefaultKey.
	t.Run("default namespace yields a usable default key", func(t *testing.T) {
		t.Parallel()

		opts, err := keyPolicyRegistryOpts(t.Context(), &config.KeyPolicy{URI: distinctKeyURL(t, 1)}, log, defaultNamespace)
		require.NoError(t, err)

		reg, err := crypto.NewKEKRegistry(opts...)
		require.NoError(t, err)
		require.NoError(t, reg.Close())
	})

	t.Run("non-default namespace has no default key", func(t *testing.T) {
		t.Parallel()

		opts, err := keyPolicyRegistryOpts(t.Context(), &config.KeyPolicy{URI: distinctKeyURL(t, 2)}, log, "other")
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

			v, err := createVault(&config.Config{Encryption: tt.enc}, reg)
			require.NoError(t, err)
			require.NotNil(t, v)
		})
	}
}

func TestCreateKEKRegistry(t *testing.T) {
	t.Parallel()

	lc := fxtest.NewLifecycle(t)
	cfg := &config.Config{Encryption: config.Encryption{Enabled: true, Default: &config.KeyPolicy{URI: distinctKeyURL(t, 1)}}}

	reg, err := createKEKRegistry(t.Context(), lc, cfg, logger.NewNoopLogger())
	require.NoError(t, err)
	require.NotNil(t, reg)

	// The registered OnStop hook closes the registry without error.
	lc.RequireStart()
	lc.RequireStop()
}

func TestModule_Disabled_ProvidesNilVault(t *testing.T) {
	t.Parallel()

	var v *crypto.Vault
	app := fx.New(
		fx.Supply(fx.Annotate(t.Context(), fx.As(new(context.Context)))),
		fx.Supply(&config.Config{Encryption: config.Encryption{Enabled: false}}),
		fx.Provide(func() logger.Logger { return logger.NewNoopLogger() }),
		Module,
		fx.Populate(&v),
		fx.NopLogger,
	)

	require.NoError(t, app.Err())
	require.Nil(t, v)
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
