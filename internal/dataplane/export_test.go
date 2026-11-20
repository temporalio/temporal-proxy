package dataplane

import (
	"go.temporal.io/proxy/internal/config"
	"go.temporal.io/proxy/internal/metrics"
)

// Reporters is the set of metric reporters one dataplane registers, exposed so
// tests can pin the exact metric surface they register.
type Reporters = reporters

// NewReporters builds the reporters for c against f, exported so tests can
// assert the metric names and labels a wired dataplane emits.
func NewReporters(f *metrics.Factory, c *config.Config, encryption bool) (Reporters, error) {
	return newReporters(f, c, encryption)
}
