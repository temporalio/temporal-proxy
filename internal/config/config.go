package config

import (
	"fmt"
	"io"
	"os"

	"github.com/goccy/go-yaml"
)

// Config holds the complete proxy configuration.
type Config struct {
	Clusters   []Cluster  `yaml:"clusters"`
	Encryption Encryption `yaml:"encryption"`
}

// Load reads and parses the YAML config specified in the Reader.
// Values of the form ${VAR} are replaced with the corresponding environment variable.
func Load(r io.Reader) (*Config, error) {
	data, err := io.ReadAll(r)
	if err != nil {
		return nil, err
	}

	expanded := os.Expand(string(data), os.Getenv)

	var cfg Config
	if err := yaml.Unmarshal([]byte(expanded), &cfg); err != nil {
		return nil, err
	}

	if err := cfg.validate(); err != nil {
		return nil, err
	}

	return &cfg, nil
}

// LoadFile reads and parses the YAML config file at path.
// Values of the form ${VAR} are replaced with the corresponding environment variable.
func LoadFile(path string) (*Config, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer func() { _ = f.Close() }()

	return Load(f)
}

func (c *Config) validate() error {
	if len(c.Clusters) == 0 {
		return fmt.Errorf("must define at least one cluster")
	}

	for i, cluster := range c.Clusters {
		if err := cluster.validate(i); err != nil {
			return err
		}
	}

	if err := c.Encryption.validate(); err != nil {
		return err
	}

	return nil
}
