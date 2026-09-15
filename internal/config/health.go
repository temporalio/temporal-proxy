package config

import (
	"cmp"
	"time"

	"github.com/temporalio/temporal-proxy/pkg/validation"
)

const (
	// defaultHealthInterval is how often the liveness check runs when the config
	// says nothing. It matches the cadence the gateway refreshed its serving
	// status on before the check existed.
	defaultHealthInterval = 30 * time.Second

	// defaultHealthTimeout bounds one check. It is well under
	// defaultHealthInterval, so a check is always finished before the next begins.
	defaultHealthTimeout = 5 * time.Second
)

// Health configures the liveness check behind the gateway's serving status.
// Enabled turns the check on or off, Interval is how often it runs, and Timeout
// bounds one run. Every field is optional: see [Health.CheckEnabled],
// [Health.CheckInterval] and [Health.CheckTimeout] for what an absent one means.
//
// Enabled is a pointer because false is a meaningful value and the default is
// true, so a plain bool could not tell an operator who wrote nothing from one
// who wrote false, and an absent field would read as a request to turn the check
// off.
type Health struct {
	Enabled  *bool         `yaml:"enabled"`
	Interval time.Duration `yaml:"interval"`
	Timeout  time.Duration `yaml:"timeout"`
}

// CheckEnabled reports whether the liveness check runs: the configured value
// when there is one, and true when the field is absent. Turning the check off
// leaves the health service in place, reporting SERVING on the same cadence, so
// a probe configured against it keeps working.
func (h *Health) CheckEnabled() bool {
	if h == nil || h.Enabled == nil {
		return true
	}

	return *h.Enabled
}

// CheckInterval is how often the check runs: the configured interval when there
// is one, and [defaultHealthInterval] when the field is absent. A zero interval
// would spin the refresh loop, so this is the only reader of the field.
func (h *Health) CheckInterval() time.Duration {
	if h == nil {
		return defaultHealthInterval
	}

	return cmp.Or(h.Interval, defaultHealthInterval)
}

// CheckTimeout bounds one run of the check: the configured timeout when there is
// one, and [defaultHealthTimeout] when the field is absent.
func (h *Health) CheckTimeout() time.Duration {
	if h == nil {
		return defaultHealthTimeout
	}

	return cmp.Or(h.Timeout, defaultHealthTimeout)
}

// Validate requires a positive interval and a timeout that is positive and
// strictly shorter than the interval, so a check can never still be running when
// the next one begins. Both are checked through their accessors, so an absent
// field is the default (which passes) while a negative one still fails. A block
// that disables the check is validated all the same: a configuration that would
// not work if it were turned on is a mistake worth reporting now.
func (h *Health) Validate() error {
	return validation.Validate(
		"",
		validation.Field("interval", h.CheckInterval(), validation.GT[time.Duration](0)),
		validation.Field("timeout", h.CheckTimeout(),
			validation.GT[time.Duration](0),
			// Only compared against an interval that is itself legal, so an operator
			// who wrote a negative interval reads that one failure rather than a
			// second one about a bound that could never have held.
			validation.WhenFn(
				func() bool { return h.CheckInterval() > 0 },
				validation.LT(h.CheckInterval()),
			),
		),
	)
}
