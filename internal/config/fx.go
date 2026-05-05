package config

import "go.uber.org/fx"

// Module provides a [Config] to the fx application by loading the YAML file
// supplied under the fx name "configFile".
var Module = fx.Option(fx.Provide(func(p ConfigParams) (*Config, error) {
	return LoadFile(p.File)
}))

// ConfigParams holds the fx-injected parameters for loading the proxy configuration.
type ConfigParams struct {
	fx.In

	// File is the path to the YAML configuration file.
	File string `name:"configFile"`
}
