package adapt

import (
	"context"
	"fmt"
	"time"

	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.uber.org/fx"
	"gocloud.dev/secrets"
	_ "gocloud.dev/secrets/awskms"
	_ "gocloud.dev/secrets/azurekeyvault"
	_ "gocloud.dev/secrets/gcpkms"
	_ "gocloud.dev/secrets/localsecrets"

	"github.com/temporalio/temporal-proxy/internal/config"
	"github.com/temporalio/temporal-proxy/internal/crypto"
)

const (
	minKeyDuration     = 1 * time.Minute  // The minimum time a DEK will be valid for.
	defaultRenewFactor = 0.1              // What percentage of key duration to use for renewBefore by default.
	rotateKeyInterval  = 30 * time.Second // How often to check for expired DEKs.
)

type (
	// cloudKey satisfies crypto.KEK by delegating to an underlying [secrets.Keeper] instance.
	cloudKey struct {
		*secrets.Keeper
		id string
	}

	// CryptoParams carries the fx output values produced by [CryptoPolicies].
	CryptoParams struct {
		fx.Out

		// DefaultPolicy is the fallback [crypto.KeyPolicy] for namespaces without an explicit entry.
		DefaultPolicy crypto.KeyPolicy

		// Policies maps Temporal namespace names to their per-namespace [crypto.KeyPolicy].
		Policies map[string]crypto.KeyPolicy
	}
)

// CryptoPolicies builds a [CryptoParams] from the encryption settings defined in the application config.
func CryptoPolicies(ctx context.Context, c *config.Config, l log.Logger) (CryptoParams, error) {
	p := CryptoParams{
		Policies: make(map[string]crypto.KeyPolicy, len(c.Encryption.Policies)),
	}

	logger := scopedLogger(l, "key-policies")

	if dp := c.Encryption.DefaultKeyPolicy; dp.URI != "" {
		logger.Info("configuring default key policy", tag.String("uri", dp.URI))
		pol, err := createPolicy(ctx, &dp)
		if err != nil {
			return p, err
		}

		p.DefaultPolicy = pol
	}

	for ns, kp := range c.Encryption.Policies {
		logger.Info("configuring key policy", tag.String("ns", ns), tag.String("uri", kp.URI))
		pol, err := createPolicy(ctx, &kp)
		if err != nil {
			return p, err
		}

		p.Policies[ns] = pol
	}

	return p, nil
}

// RotateDEKs starts a background goroutine that calls [crypto.Sealer.Refresh] every 30 seconds
// until ctx is cancelled, logging a warning if any rotation fails.
func RotateDEKs(ctx context.Context, s *crypto.Sealer, l log.Logger) {
	logger := scopedLogger(l, "dek-rotator")

	go func() {
		// initial creation at startup
		logger.Debug("[adapt]: rotating expired DEKs")
		if err := s.Refresh(); err != nil {
			logger.Warn("[adapt]: rotation failed", tag.Error(err))
		}

		for {
			select {
			case <-ctx.Done():
				return
			case <-time.After(rotateKeyInterval):
				// NB: Don't bother if we're done.
				if ctx.Err() != nil {
					return
				}

				logger.Debug("[adapt]: rotating expired DEKs")
				if err := s.Refresh(); err != nil {
					logger.Warn("[adapt]: rotation failed", tag.Error(err))
				}
			}
		}
	}()
}

func (k *cloudKey) ID() string {
	return k.id
}

func createPolicy(ctx context.Context, kp *config.KeyPolicy) (crypto.KeyPolicy, error) {
	k, err := secrets.OpenKeeper(ctx, kp.URI)
	if err != nil {
		return crypto.KeyPolicy{}, fmt.Errorf("failed to create KEK: %s, %w", kp.URI, err)
	}

	pol := crypto.KeyPolicy{
		KEK:         &cloudKey{Keeper: k, id: kp.URI},
		Duration:    max(kp.Duration, minKeyDuration),
		RenewBefore: kp.RenewBefore,
	}

	// When no renewBefore is specified, use the default of 10% of the duration.
	if pol.RenewBefore < 0 {
		pol.RenewBefore = time.Duration(float64(pol.Duration) * defaultRenewFactor)
	}

	return pol, nil
}

func scopedLogger(l log.Logger, name string) log.Logger {
	return log.With(
		l,
		tag.String("pkg", "adapt"),
		tag.String("component", name),
	)
}
