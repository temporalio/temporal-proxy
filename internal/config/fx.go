package config

import "go.uber.org/fx"

var Module = fx.Option(fx.Provide(func(p ConfigParams) (*Config, error) {
	return LoadFile(p.File)
}))

type ConfigParams struct {
	fx.In
	File string `name:"configFile"`
}
