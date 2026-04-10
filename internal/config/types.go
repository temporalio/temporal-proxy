package config

import (
	"fmt"
)

type (
	Config struct {
		Listen ListenConfig `yaml:"listen"`
	}

	ListenConfig struct {
		HostPort string `yaml:"hostPort"`
		TLS      *TLS   `yaml:"tls"`
	}

	TLS struct {
		Cert       string `yaml:"cert"`
		Key        string `yaml:"key"`
		CA         string `yaml:"ca"`
		ServerName string `yaml:"serverName"`
	}
)

func (c *Config) validate() error {
	if c.Listen.HostPort == "" {
		return fmt.Errorf("listen.hostPort is required")
	}

	return nil
}
