package config

import (
	"go.temporal.io/proxy/pkg/validation"
)

// PProf configures the pprof routes, which serve the runtime's profiles on
// the standard net/http/pprof endpoints under /debug/pprof. Profiles can name
// hot code paths, so the routes are off unless enabled, and they belong behind
// authentication whenever the listener is reachable beyond loopback, which
// [HTTP.Validate] enforces. The listener itself owns the address and TLS.
type PProf struct {
	Enabled bool        `yaml:"enabled"`
	Auth    *AuthConfig `yaml:"auth"`
}

// Validate checks the pprof routes' authentication. A disabled block is not
// checked at all, so a half-written one can be left in place. Whether auth is
// required depends on the listener, so [HTTP.Validate] checks that.
func (p *PProf) Validate() error {
	if !p.Enabled {
		return nil
	}

	return validation.Validate(
		"",
		validation.WhenNested(func() bool { return p.Auth != nil }, "auth", p.Auth),
	)
}

// enabled reports whether the pprof routes are served.
func (p *PProf) enabled() bool { return p.Enabled }

// authConfig returns the pprof routes' caller authentication, or nil.
func (p *PProf) authConfig() *AuthConfig { return p.Auth }
