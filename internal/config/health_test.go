package config_test

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/temporalio/temporal-proxy/internal/config"
	"github.com/temporalio/temporal-proxy/pkg/validation"
)

func TestHealth_Defaults(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		yaml         string
		wantEnabled  bool
		wantInterval time.Duration
		wantTimeout  time.Duration
	}{
		{
			name:         "absent health block runs the check with both defaults",
			yaml:         "hostPort: :8080\n",
			wantEnabled:  true,
			wantInterval: 30 * time.Second,
			wantTimeout:  5 * time.Second,
		},
		{
			name:         "an explicit false disables the check",
			yaml:         "health:\n  enabled: false\n",
			wantEnabled:  false,
			wantInterval: 30 * time.Second,
			wantTimeout:  5 * time.Second,
		},
		{
			name:         "explicit values are preserved",
			yaml:         "health:\n  enabled: true\n  interval: 10s\n  timeout: 2s\n",
			wantEnabled:  true,
			wantInterval: 10 * time.Second,
			wantTimeout:  2 * time.Second,
		},
		{
			name:         "each duration defaults on its own",
			yaml:         "health:\n  interval: 10s\n",
			wantEnabled:  true,
			wantInterval: 10 * time.Second,
			wantTimeout:  5 * time.Second,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cfg, err := config.Load(strings.NewReader(tt.yaml))
			require.NoError(t, err)
			require.Equal(t, tt.wantEnabled, cfg.Health.CheckEnabled())
			require.Equal(t, tt.wantInterval, cfg.Health.CheckInterval())
			require.Equal(t, tt.wantTimeout, cfg.Health.CheckTimeout())
		})
	}
}

func TestHealth_Validate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		cfg      config.Health
		wantErrs []validation.Error
	}{
		{
			name: "a zero block is valid, since both durations default",
			cfg:  config.Health{},
		},
		{
			name: "a timeout shorter than the interval is valid",
			cfg:  config.Health{Interval: time.Second, Timeout: 500 * time.Millisecond},
		},
		{
			name:     "a timeout equal to the interval is rejected",
			cfg:      config.Health{Interval: time.Second, Timeout: time.Second},
			wantErrs: []validation.Error{{Field: "timeout", Message: "not less than 1s"}},
		},
		{
			name:     "a timeout longer than the interval is rejected",
			cfg:      config.Health{Interval: time.Second, Timeout: 2 * time.Second},
			wantErrs: []validation.Error{{Field: "timeout", Message: "not less than 1s"}},
		},
		{
			name:     "a negative interval is rejected",
			cfg:      config.Health{Interval: -time.Second},
			wantErrs: []validation.Error{{Field: "interval", Message: "not greater than 0s"}},
		},
		{
			name: "a negative timeout is rejected",
			cfg:  config.Health{Timeout: -time.Second},
			wantErrs: []validation.Error{
				{Field: "timeout", Message: "not greater than 0s"},
			},
		},
		{
			// The defaults are themselves a valid pair, so a block that disables the
			// check is never rejected for durations nothing will read.
			name: "a disabled check is still validated",
			cfg:  config.Health{Enabled: new(false), Interval: time.Second, Timeout: 2 * time.Second},
			wantErrs: []validation.Error{
				{Field: "timeout", Message: "not less than 1s"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := tt.cfg.Validate()
			if len(tt.wantErrs) == 0 {
				require.NoError(t, err)
				return
			}

			var errs validation.Errors
			require.True(t, errors.As(err, &errs), "expected validation.Errors, got %T", err)
			require.ElementsMatch(t, tt.wantErrs, []validation.Error(errs))
		})
	}
}
