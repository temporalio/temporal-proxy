package config

import (
	"fmt"
)

const (
	// Inbound means the remote sends RPCs to this proxy.
	Inbound RemoteType = "inbound"

	// Outbound means the remote receives RPCs from this proxy.
	Outbound RemoteType = "outbound"
)

type (
	Config struct {
		Clusters []Cluster `yaml:"clusters"`
	}

	// Cluster defines a cluster involved with this proxy.
	Cluster struct {
		Local  LocalCluster  `yaml:"local"`
		Remote RemoteCluster `yaml:"remote"`
	}

	// LocalCluster defines connection details for ingress and proxying on this proxy instance.
	//
	// RPCs received on the outbound listener, are forwarded to the remote.
	LocalCluster struct {
		Inbound  ListenConfig `yaml:"inbound"`
		Outbound ListenConfig `yaml:"outbound"` // NB: Temporal client/K8s service should point here.
	}

	// RemoteCluster defines a cluster that this proxy either sends RPCs to (outbound) or receives RPCs from (inbound).
	RemoteCluster struct {
		Name     string       `yaml:"name"`
		Type     RemoteType   `yaml:"type"`
		PoolSize int          `yaml:"poolSize"`
		Listener ListenConfig `yaml:",inline"`
	}

	// RemoteType defines the type of remote (inbound or outbound).
	RemoteType string

	// ListenConfig defines properties for a listener.
	ListenConfig struct {
		HostPort string `yaml:"hostPort"`
		TLS      *TLS   `yaml:"tls"`
	}

	// TLS defines details about TLS connections.
	TLS struct {
		Cert               string `yaml:"cert"`
		Key                string `yaml:"key"`
		CA                 string `yaml:"ca"`
		ServerName         string `yaml:"serverName"`
		InsecureSkipVerify bool   `yaml:"skipVerify"`
	}
)

func (c *Config) validate() error {
	if len(c.Clusters) == 0 {
		return fmt.Errorf("must define at least one cluster")
	}

	return nil
}
