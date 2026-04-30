package config

import (
	"fmt"
)

const (
	// Inbound means the remote sends RPCs to this proxy.
	Inbound ClusterType = "inbound"

	// Outbound means the remote receives RPCs from this proxy.
	Outbound ClusterType = "outbound"

	// Temporal means the remote is a real Temporal server reached via plain gRPC.
	Temporal ClusterType = "temporal"
)

type (
	Config struct {
		Clusters []Cluster     `yaml:"clusters"`
		Metrics  MetricsConfig `yaml:"metrics"`
	}

	// Cluster defines a cluster involved with this proxy.
	Cluster struct {
		Name     string       `yaml:"name"`
		Type     ClusterType  `yaml:"type"`
		Listener ListenConfig `yaml:"listener"`
		Upstream Upstream     `yaml:"upstream"`
	}

	// ClusterType defines the relationship between the local and remote sides of a cluster.
	ClusterType string

	// ListenConfig defines properties for a listener.
	ListenConfig struct {
		HostPort string `yaml:"hostPort"`
		TLS      *TLS   `yaml:"tls"`
	}

	// MetricsConfig defines the Prometheus HTTP endpoint.
	MetricsConfig struct {
		HostPort string `yaml:"hostPort"`
		Path     string `yaml:"path"`
	}

	// TLS defines details about TLS connections.
	TLS struct {
		Cert               string `yaml:"cert"`
		Key                string `yaml:"key"`
		CA                 string `yaml:"ca"`
		ServerName         string `yaml:"serverName"`
		InsecureSkipVerify bool   `yaml:"skipVerify"`
	}

	// Upstream defines connection details for the cluster this proxy communicates with.
	Upstream struct {
		PoolSize int          `yaml:"poolSize"`
		Listener ListenConfig `yaml:",inline"`
	}
)

func (c *Config) validate() error {
	if len(c.Clusters) == 0 {
		return fmt.Errorf("must define at least one cluster")
	}

	for i, cluster := range c.Clusters {
		if err := cluster.validate(i); err != nil {
			return err
		}
	}

	return nil
}

func (c *Cluster) validate(idx int) error {
	switch c.Type {
	case Inbound:
		if c.Listener.HostPort == "" {
			return fmt.Errorf("cluster[%d]: listener.hostPort required for cluster type %q", idx, c.Type)
		}
	case Outbound:
		if c.Listener.HostPort == "" {
			return fmt.Errorf("cluster[%d]: listener.hostPort required for cluster type %q", idx, c.Type)
		}
		if c.Upstream.Listener.HostPort == "" {
			return fmt.Errorf("cluster[%d]: upstream.hostPort required for cluster type %q", idx, c.Type)
		}
	case Temporal:
		if c.Listener.HostPort == "" {
			return fmt.Errorf("cluster[%d]: listener.hostPort required for cluster type %q", idx, c.Type)
		}
		if c.Upstream.Listener.HostPort == "" {
			return fmt.Errorf("cluster[%d]: upstream.hostPort required for cluster type %q", idx, c.Type)
		}
	case "":
		return fmt.Errorf("cluster[%d]: type is required", idx)
	default:
		return fmt.Errorf("cluster[%d]: unknown cluster type %q", idx, c.Type)
	}

	return nil
}
