package kms

import (
	"context"
	"fmt"
	"net/url"
	"time"

	"go.uber.org/fx"

	"github.com/temporalio/temporal-proxy/internal/config"
	"github.com/temporalio/temporal-proxy/pkg/crypto"
	"github.com/temporalio/temporal-proxy/pkg/logger"
	"github.com/temporalio/temporal-proxy/pkg/logger/tag"
)

const (
	defaultNamespace = "default"
	rotationInterval = 10 * time.Second
)

// Module provides a *crypto.Vault built from the proxy's encryption
// configuration and, when encryption is enabled, runs background key rotation
// for the lifetime of the fx application. When encryption is disabled the
// provided vault is nil and no rotation goroutine is started.
var Module = fx.Options(
	fx.Provide(
		func(p KMSParams) (*crypto.Vault, error) {
			if !p.Config.Encryption.Enabled {
				return nil, nil
			}

			r, err := createKEKRegistry(
				p.Context,
				p.Lifecycle,
				p.Config,
				p.Logger,
			)
			if err != nil {
				return nil, err
			}

			v, err := createVault(p.Config, r)
			if err != nil {
				_ = r.Close()
				return nil, err
			}

			return v, nil
		},
	),
	fx.Invoke(func(p KMSParams, v *crypto.Vault) {
		if !p.Config.Encryption.Enabled {
			return
		}

		ctx, cancel := context.WithCancel(p.Context)

		p.Lifecycle.Append(fx.Hook{
			OnStart: func(context.Context) error {
				go runRotation(ctx, v, rotationInterval, p.Logger)
				return nil
			},
			OnStop: func(context.Context) error {
				cancel()
				return nil
			},
		})
	}),
)

type (
	// KMSParams are the fx dependencies used to construct and run the vault.
	KMSParams struct {
		fx.In

		Context   context.Context
		Config    *config.Config
		Lifecycle fx.Lifecycle
		Logger    logger.Logger
	}

	// vaultRefresher is the subset of *crypto.Vault the rotation loop depends
	// on. It exists so runRotation can be exercised with a stub vault and a fake
	// clock.
	vaultRefresher interface {
		Refresh() error
	}
)

// runRotation refreshes v once immediately, then again on every interval until
// ctx is cancelled. A failed refresh is logged and does not stop the loop, so a
// transient KMS outage does not permanently halt rotation. It is intended to
// run in its own goroutine for the lifetime of the process.
func runRotation(ctx context.Context, v vaultRefresher, interval time.Duration, log logger.Logger) {
	refresh := func() {
		// Debug, not Info: this fires every interval for the life of the process,
		// so at Info it would flood logs even when nothing rotates.
		log.Debug("Rotating expired keys")
		if err := v.Refresh(); err != nil {
			log.Warn("Key rotation failed", tag.Error(err))
		}
	}

	refresh()

	t := time.NewTicker(interval)
	defer t.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Debug("Stopping key rotation routine")
			return
		case <-t.C:
			refresh()
		}
	}
}

// createVault builds a vault from the registry, applying the configured cache
// size and, when a default key policy is set, its DEK duration and renewal lead
// time.
func createVault(c *config.Config, r *crypto.KEKRegistry) (*crypto.Vault, error) {
	opts := make([]crypto.VaultOption, 0, 2)
	opts = append(opts, crypto.WithCacheSize(c.Encryption.CacheSize))

	if dp := c.Encryption.Default; dp != nil {
		opts = append(opts, crypto.WithDefaultKeyConfig(crypto.KeyConfig{
			Duration:    dp.Duration,
			RenewBefore: dp.RenewBefore,
		}))
	}

	v, err := crypto.NewVault(r, opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to create crypto vault: %w", err)
	}

	return v, nil
}

// createKEKRegistry opens the configured KEKs and assembles a registry,
// registering an fx OnStop hook that closes the registry (and its KEKs) on
// shutdown.
func createKEKRegistry(ctx context.Context, lc fx.Lifecycle, c *config.Config, logger logger.Logger) (*crypto.KEKRegistry, error) {
	opts := []crypto.KEKRegistryOption{}
	if dp := c.Encryption.Default; dp != nil {
		res, err := keyPolicyRegistryOpts(ctx, dp, logger, defaultNamespace)
		if err != nil {
			return nil, err
		}

		opts = append(opts, res...)
	}

	registry, err := crypto.NewKEKRegistry(opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to create KEK registry: %w", err)
	}

	lc.Append(fx.Hook{
		// NB: Ensure we close the registry on shutdown
		OnStop: func(context.Context) error { return registry.Close() },
	})

	return registry, nil
}

// keyPolicyRegistryOpts turns a single key policy into registry options for the
// given namespace: the policy's primary URI becomes the namespace's encryption
// key (the default key when ns is the default namespace), and every DecryptURIs
// entry is registered decrypt-only.
func keyPolicyRegistryOpts(
	ctx context.Context,
	p *config.KeyPolicy,
	log logger.Logger,
	ns string,
) ([]crypto.KEKRegistryOption, error) {
	opts := []crypto.KEKRegistryOption{}
	keys, err := createKEKs(ctx, p, log, ns)
	if err != nil {
		return nil, err
	}

	// NB: KeyConfig.URI is required and therefore this will never be out of bounds.
	if ns == defaultNamespace {
		opts = append(opts, crypto.WithDefaultKey(keys[0]))
	} else {
		opts = append(opts, crypto.WithKeyForNamespace(ns, keys[0]))
	}

	for i := 1; i < len(keys); i++ {
		opts = append(opts, crypto.WithDecryptOnlyKey(keys[i]))
	}

	return opts, nil
}

// createKEKs opens the policy's primary URI followed by each of its DecryptURIs,
// returning the KEKs in that order. The primary key is always element zero. If
// any key fails to open, every key opened so far is closed before returning, so
// a partial failure leaks no keepers.
func createKEKs(ctx context.Context, p *config.KeyPolicy, log logger.Logger, ns string) (_ []crypto.KEK, err error) {
	log = log.With(tag.String("namespace", ns))
	keys := make([]crypto.KEK, 0, len(p.DecryptURIs)+1)

	defer func() {
		if err != nil {
			for _, k := range keys {
				_ = k.Close()
			}
		}
	}()

	mkKey := func(uri *url.URL) error {
		log.Info("Registering crypto key", tag.String("uri", safeKeyString(uri.String())))
		k, err := newKEK(ctx, uri.String())
		if err != nil {
			return err
		}

		keys = append(keys, k)
		return nil
	}

	if err = mkKey(&p.URI); err != nil {
		return nil, err
	}

	for _, uri := range p.DecryptURIs {
		if err = mkKey(&uri); err != nil {
			return nil, err
		}
	}

	return keys, nil
}
