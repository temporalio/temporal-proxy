package config

import "go.uber.org/fx"

// ConfigFileTag annotates a provided string as the named value "configFile",
// the config file path [Module] loads.
var ConfigFileTag = fx.ResultTags(`name:"configFile"`)

// Module is an fx module that provides *Config, loaded and prepared from the
// file path supplied as the named value "configFile", along with the allowlist derived from it via
// [NewAllowlist], which is the single owner of how the allowlist is built.
var Module = fx.Option(fx.Provide(
	func(p ConfigParams) (*Config, error) {
		return LoadFile(p.File)
	},
	NewAllowlist,
))

// ConfigParams holds the fx-injected dependencies for loading the config file.
type ConfigParams struct {
	fx.In
	File string `name:"configFile"`
}

// Supply provides cfg, prepared, and the allowlist derived from it, for an
// application that builds its Config in code rather than loading a file. A
// config that fails [Config.Prepare] fails the graph. It prepares cfg in place,
// including slice elements, and is not safe for concurrent use.
func Supply(cfg *Config) fx.Option {
	return fx.Provide(
		func() (*Config, error) {
			if err := cfg.Prepare(); err != nil {
				return nil, err
			}

			return cfg, nil
		},
		NewAllowlist,
	)
}
