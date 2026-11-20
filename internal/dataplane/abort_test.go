package dataplane_test

import (
	"strings"
	"testing"

	"go.uber.org/fx"

	"go.temporal.io/proxy/internal/config"
	"go.temporal.io/proxy/internal/dataplane"
)

// abortConfig points the only upstream at a template, so the plane serves
// without anything having to be reachable during Start.
func abortConfig(t *testing.T) *config.Config {
	t.Helper()

	cfg := testConfig()
	cfg.Upstreams[0].Listen.HostPort = "{{ .LocalNamespace }}." + strings.ToLower(t.Name()) + ".example:7233"

	return cfg
}

// abortTo replaces the Abort Module provides with one that sends to aborts, so a
// test observes the stop rather than having it shut an application down.
func abortTo(aborts chan<- error) fx.Option {
	return fx.Decorate(func() dataplane.Abort {
		return func(err error) { aborts <- err }
	})
}
