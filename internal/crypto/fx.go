package crypto

import (
	"go.uber.org/fx"
)

var Module = fx.Options(fx.Provide(
	func(p KEKRegistryParams) *KEKRegistry {
		opts := make([]KEKRegistryOption, 0, len(p.NamespaceKeys)+1)
		opts = append(opts, WithDefaultKey(p.DefaultKey))

		for k, v := range p.NamespaceKeys {
			opts = append(opts, WithKeyForNamespace(k, v))
		}

		return NewKEKRegistry(opts...)
	},
	func(p SealerParams) (*Sealer, error) {
		opts := make([]SealerOption, 0, len(p.KeyConfigs))
		for k, v := range p.KeyConfigs {
			opts = append(opts, WithKeyConfig(k, v))
		}

		return NewSealer(p.Registry, opts...)
	},
))

type (
	// KEKRegistryParams holds the fx-injected parameters for constructing a KEKRegistry.
	KEKRegistryParams struct {
		fx.In

		// Default key to use when namespace override doesn't exist.
		// When nil, a blank KEK is used (no encryption occurs).
		DefaultKey KEK `optional:"true"`

		// A map of namespace to KEK.
		NamespaceKeys map[string]KEK
	}

	// SealerParams holds the fx-injected parameters for constructing a [Sealer].
	SealerParams struct {
		fx.In

		// KeyConfigs maps each namespace ID to its [KeyConfig] rotation policy.
		KeyConfigs map[string]KeyConfig
		// Registry is the [KEKRegistry] used to encrypt and decrypt DEKs.
		Registry *KEKRegistry
	}
)
