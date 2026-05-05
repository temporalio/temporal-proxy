package crypto

import (
	"go.uber.org/fx"
)

var Module = fx.Options(fx.Provide(
	func(p CryptoParams) (*Sealer, error) {
		opts := make([]KEKRegistryOption, 0, len(p.Policies)+1)
		opts = append(opts, WithDefaultPolicy(p.DefaultPolicy))

		for ns, policy := range p.Policies {
			opts = append(opts, WithKeyPolicy(ns, policy))
		}

		return NewSealer(NewKEKRegistry(opts...))
	},
))

// CryptoParams holds the fx-injected parameters for constructing a KEKRegistry.
type CryptoParams struct {
	fx.In

	// DefaultPolicy is the fallback used when no namespace-specific policy is registered.
	// When zero, a no-op KEK is used (no encryption occurs).
	DefaultPolicy KeyPolicy `optional:"true"`

	// Policies maps each namespace to its [KeyPolicy].
	Policies map[string]KeyPolicy
}
