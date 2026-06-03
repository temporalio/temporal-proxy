package config

import "go.uber.org/fx"

var ConfigFileTag = fx.ResultTags(`name:"configFile"`)

// Module is an fx module that provides *Config by loading the file path
// supplied as the named value "configFile".
var Module = fx.Option(fx.Provide(func(p ConfigParams) (*Config, error) {
	return LoadFile(p.File)
}))

// ConfigParams holds the fx-injected dependencies for loading the config file.
type ConfigParams struct {
	fx.In
	File string `name:"configFile"`
}
