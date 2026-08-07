package config

import "go.uber.org/fx"

var ConfigFileTag = fx.ResultTags(`name:"configFile"`)

// Module is an fx module that provides *Config by loading the file path supplied
// as the named value "configFile", along with the allowlist derived from it via
// [NewAllowlist], which is the single owner of how the forwarding gate is built.
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
