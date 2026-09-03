package config_test

import (
	"errors"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/temporalio/temporal-proxy/internal/config"
	"github.com/temporalio/temporal-proxy/pkg/validation"
)

func TestMetrics_Validate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		cfg      *config.Metrics
		wantErrs []validation.Error
	}{}

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

func TestLoad_MetricsDefaults(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		yaml string
		want config.Metrics
	}{
		{
			name: "absent metrics block gets both defaults",
			yaml: "hostPort: :8080\n",
			want: config.Metrics{HostPort: ":9090", Namespace: "tmprl_proxy"},
		},
		{
			name: "explicit values are preserved",
			yaml: "metrics:\n  hostPort: 127.0.0.1:8888\n  namespace: acme\n",
			want: config.Metrics{HostPort: "127.0.0.1:8888", Namespace: "acme"},
		},
		{
			name: "each field defaults on its own",
			yaml: "metrics:\n  hostPort: :7070\n",
			want: config.Metrics{HostPort: ":7070", Namespace: "tmprl_proxy"},
		},
		{
			name: "namespace only",
			yaml: "metrics:\n  namespace: acme\n",
			want: config.Metrics{HostPort: ":9090", Namespace: "acme"},
		},
		{
			// cmp.Or cannot tell an explicit empty string from an absent key, so
			// writing "" is not a way to opt out of the default.
			name: "explicit empty strings still default",
			yaml: "metrics:\n  hostPort: \"\"\n  namespace: \"\"\n",
			want: config.Metrics{HostPort: ":9090", Namespace: "tmprl_proxy"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := config.Load(strings.NewReader(tt.yaml))
			require.NoError(t, err)
			require.Equal(t, tt.want, got.Metrics)
		})
	}
}
