package config

import "github.com/temporalio/temporal-proxy/pkg/validation"

// Metrics configures the Prometheus endpoint. HostPort is the address the
// /metrics handler listens on, and Namespace is the prefix stamped onto every
// collector: a Prometheus namespace, unrelated to a Temporal namespace. Load
// defaults both, so neither is empty in a loaded config.
type Metrics struct {
	HostPort  string `yaml:"hostPort"`
	Namespace string `yaml:"namespace"`
}

// Validate requires a valid host:port and a non-empty namespace. Load defaults
// both, so a namespace failure is only reachable for a Metrics built directly,
// and a hostPort failure only for a config that sets one that will not parse.
func (m *Metrics) Validate() error {
	return validation.Validate(
		"",
		validation.Field("hostPort", m.HostPort, validation.IsHostPort()),
		validation.Field("namespace", m.Namespace, validation.Required[string]()),
	)
}
