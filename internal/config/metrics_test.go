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
	}{
		{
			name: "port-only hostPort and a namespace",
			cfg:  &config.Metrics{HostPort: ":9090", Namespace: "tmprl_proxy"},
		},
		{
			name: "host and port",
			cfg:  &config.Metrics{HostPort: "127.0.0.1:9090", Namespace: "tmprl_proxy"},
		},
		{
			name: "hostPort without a port",
			cfg:  &config.Metrics{HostPort: "localhost", Namespace: "tmprl_proxy"},
			wantErrs: []validation.Error{
				{Field: "hostPort", Message: "is not a valid host:port"},
			},
		},
		{
			name: "missing namespace",
			cfg:  &config.Metrics{HostPort: ":9090"},
			wantErrs: []validation.Error{
				{Field: "namespace", Message: "is required"},
			},
		},
		{
			// The zero value only reaches Validate when a Metrics is built
			// directly; Load defaults both fields before anything sees it.
			name: "zero value fails on both fields",
			cfg:  &config.Metrics{},
			wantErrs: []validation.Error{
				{Field: "hostPort", Message: "is not a valid host:port"},
				{Field: "namespace", Message: "is required"},
			},
		},
		{
			name: "distinct metadata labels",
			cfg: &config.Metrics{
				HostPort:  ":9090",
				Namespace: "tmprl_proxy",
				Labels: config.MetricLabels{Metadata: []config.MetricLabel{
					{Header: "x-tenant", Name: "tenant"},
					{Header: "x-region", Name: "region"},
				}},
			},
		},
		{
			// Prometheus refuses to register a collector with a duplicate label
			// name, so two headers may not share one name.
			name: "two metadata labels sharing a name",
			cfg: &config.Metrics{
				HostPort:  ":9090",
				Namespace: "tmprl_proxy",
				Labels: config.MetricLabels{Metadata: []config.MetricLabel{
					{Header: "x-a", Name: "dup"},
					{Header: "x-b", Name: "dup"},
				}},
			},
			wantErrs: []validation.Error{
				{Subject: "labels", Field: "metadata[name]", Message: "contains duplicate value: dup"},
			},
		},
		{
			name: "the same header twice under different names is allowed",
			cfg: &config.Metrics{
				HostPort:  ":9090",
				Namespace: "tmprl_proxy",
				Labels: config.MetricLabels{Metadata: []config.MetricLabel{
					{Header: "x-a", Name: "one"},
					{Header: "x-a", Name: "two"},
				}},
			},
		},
		{
			name: "a broken metadata label carries its index as the subject",
			cfg: &config.Metrics{
				HostPort:  ":9090",
				Namespace: "tmprl_proxy",
				Labels: config.MetricLabels{Metadata: []config.MetricLabel{
					{Header: "x-ok", Name: "ok"},
					{Header: "x-bad", Name: "not a label"},
				}},
			},
			wantErrs: []validation.Error{{
				Subject: "labels.metadata[1]",
				Field:   "name",
				Message: `is not valid, must match: "^[a-zA-Z_][a-zA-Z0-9_]*$"`,
			}},
		},
		{
			name: "a fixed label name outside the Prometheus grammar",
			cfg: &config.Metrics{
				HostPort:  ":9090",
				Namespace: "tmprl_proxy",
				Labels:    config.MetricLabels{Fixed: map[string]string{"not-a-label": "us-west-2"}},
			},
			wantErrs: []validation.Error{{
				Subject: "labels",
				Field:   "fixed[not-a-label]",
				Message: `is not valid, must match: "^[a-zA-Z_][a-zA-Z0-9_]*$"`,
			}},
		},
		{
			name: "fixed labels alongside a metadata label",
			cfg: &config.Metrics{
				HostPort:  ":9090",
				Namespace: "tmprl_proxy",
				Labels: config.MetricLabels{
					Fixed:    map[string]string{"region": "us-west-2", "zone": "us-west-2a"},
					Metadata: []config.MetricLabel{{Header: "x-tenant", Name: "tenant"}},
				},
			},
		},
		{
			// Prometheus refuses a collector whose constant labels collide with
			// its variable ones, so the two sets have to be disjoint.
			name: "a fixed label taking a metadata label's name",
			cfg: &config.Metrics{
				HostPort:  ":9090",
				Namespace: "tmprl_proxy",
				Labels: config.MetricLabels{
					Fixed:    map[string]string{"tenant": "acme"},
					Metadata: []config.MetricLabel{{Header: "x-tenant", Name: "tenant"}},
				},
			},
			wantErrs: []validation.Error{{
				Subject: "labels",
				Field:   "fixed[tenant]",
				Message: "must not also name a metadata label",
			}},
		},
		{
			// Blank reads as the label not being there, so it asks for nothing
			// while looking like it asked for something.
			name: "a fixed label with no value",
			cfg: &config.Metrics{
				HostPort:  ":9090",
				Namespace: "tmprl_proxy",
				Labels:    config.MetricLabels{Fixed: map[string]string{"region": ""}},
			},
			wantErrs: []validation.Error{{
				Subject: "labels",
				Field:   "fixed[region]",
				Message: "must have a value",
			}},
		},
		{
			name: "a fixed label under a name Prometheus reserves",
			cfg: &config.Metrics{
				HostPort:  ":9090",
				Namespace: "tmprl_proxy",
				Labels:    config.MetricLabels{Fixed: map[string]string{"__region": "us-west-2", "le": "0.5"}},
			},
			wantErrs: []validation.Error{
				{
					Subject: "labels",
					Field:   "fixed[__region]",
					Message: `must not begin with "__", which Prometheus reserves`,
				},
				{
					Subject: "labels",
					Field:   "fixed[le]",
					Message: `must not be "le", which Prometheus reserves`,
				},
			},
		},
		{
			// Both halves of one entry can fail at once, and they report under
			// the same subject with different messages.
			name: "a fixed label with a bad name and no value",
			cfg: &config.Metrics{
				HostPort:  ":9090",
				Namespace: "tmprl_proxy",
				Labels:    config.MetricLabels{Fixed: map[string]string{"1region": ""}},
			},
			wantErrs: []validation.Error{
				{
					Subject: "labels",
					Field:   "fixed[1region]",
					Message: `is not valid, must match: "^[a-zA-Z_][a-zA-Z0-9_]*$"`,
				},
				{
					Subject: "labels",
					Field:   "fixed[1region]",
					Message: "must have a value",
				},
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
			name: "the namespace label control is read from yaml",
			yaml: "metrics:\n  labels:\n    namespace: true\n",
			want: config.Metrics{
				HostPort:  ":9090",
				Namespace: "tmprl_proxy",
				Labels:    config.MetricLabels{Namespace: true},
			},
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

func TestParseMetricLabel(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		in      string
		want    config.MetricLabel
		wantErr bool
	}{
		{
			name: "header and name",
			in:   "x-tenant:tenant",
			want: config.MetricLabel{Header: "x-tenant", Name: "tenant"},
		},
		{
			name: "whitespace around each half is trimmed",
			in:   "  x-tenant : tenant  ",
			want: config.MetricLabel{Header: "x-tenant", Name: "tenant"},
		},
		{
			// Cut splits on the first colon, so the rest lands in the name and
			// is rejected by Validate rather than here.
			name: "splits on the first colon only",
			in:   "x-h:a:b",
			want: config.MetricLabel{Header: "x-h", Name: "a:b"},
		},
		{
			// Parsing accepts an empty half; Validate is what requires both.
			name: "empty header",
			in:   ":tenant",
			want: config.MetricLabel{Name: "tenant"},
		},
		{
			name: "empty name",
			in:   "x-tenant:",
			want: config.MetricLabel{Header: "x-tenant"},
		},
		{
			name:    "no colon",
			in:      "x-tenant",
			wantErr: true,
		},
		{
			name:    "empty string",
			in:      "",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := config.ParseMetricLabel(tt.in)
			if tt.wantErr {
				require.ErrorContains(t, err, "invalid metric label")
				return
			}

			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestMetricLabel_Validate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		label    config.MetricLabel
		wantErrs []validation.Error
	}{
		{
			name:  "a metadata key and a label name",
			label: config.MetricLabel{Header: "x-tenant", Name: "tenant"},
		},
		{
			name:  "dots, dashes, and mixed case in the header",
			label: config.MetricLabel{Header: "X-Acme.Tenant-ID", Name: "tenant"},
		},
		{
			name:  "a single leading underscore is a legal label",
			label: config.MetricLabel{Header: "x-h", Name: "_tenant"},
		},
		{
			// Reported once: match yields nothing for an empty value so the
			// Required check on the same field owns that case.
			name:  "empty header reports only that it is required",
			label: config.MetricLabel{Name: "tenant"},
			wantErrs: []validation.Error{
				{Field: "header", Message: "is required"},
			},
		},
		{
			name:  "empty name reports only that it is required",
			label: config.MetricLabel{Header: "x-tenant"},
			wantErrs: []validation.Error{
				{Field: "name", Message: "is required"},
			},
		},
		{
			name:  "a space is not legal in a metadata key",
			label: config.MetricLabel{Header: "bad header", Name: "tenant"},
			wantErrs: []validation.Error{
				{Field: "header", Message: `is not valid, must match: "^[a-zA-Z0-9._-]+$"`},
			},
		},
		{
			name:  "a dash is not legal in a label name",
			label: config.MetricLabel{Header: "x-h", Name: "not-a-label"},
			wantErrs: []validation.Error{
				{Field: "name", Message: `is not valid, must match: "^[a-zA-Z_][a-zA-Z0-9_]*$"`},
			},
		},
		{
			name:  "a label may not start with a digit",
			label: config.MetricLabel{Header: "x-h", Name: "1tenant"},
			wantErrs: []validation.Error{
				{Field: "name", Message: `is not valid, must match: "^[a-zA-Z_][a-zA-Z0-9_]*$"`},
			},
		},
		{
			// gRPC reserves the "grpc-" prefix for its own metadata.
			name:  "a reserved header",
			label: config.MetricLabel{Header: "grpc-status", Name: "tenant"},
			wantErrs: []validation.Error{
				{Field: "header", Message: `must not begin with "grpc-", which gRPC reserves`},
			},
		},
		{
			// Metadata keys are case-insensitive, so the prefix check must be
			// too or this spelling would slip through.
			name:  "a reserved header in mixed case",
			label: config.MetricLabel{Header: "GRPC-Status", Name: "tenant"},
			wantErrs: []validation.Error{
				{Field: "header", Message: `must not begin with "grpc-", which gRPC reserves`},
			},
		},
		{
			name:  "only the prefix is reserved, not the substring",
			label: config.MetricLabel{Header: "x-grpc-tenant", Name: "tenant"},
		},
		{
			name:  "a header merely starting with grpc is allowed",
			label: config.MetricLabel{Header: "grpcfoo", Name: "tenant"},
		},
		{
			// gRPC marks a binary value with the "-bin" suffix, and binary is
			// not valid UTF-8, so it cannot be a label value.
			name:  "a binary metadata key",
			label: config.MetricLabel{Header: "x-trace-bin", Name: "tenant"},
			wantErrs: []validation.Error{
				{Field: "header", Message: `must not end with "-bin", which gRPC uses to mark binary metadata`},
			},
		},
		{
			name:  "a binary metadata key in mixed case",
			label: config.MetricLabel{Header: "X-Trace-BIN", Name: "tenant"},
			wantErrs: []validation.Error{
				{Field: "header", Message: `must not end with "-bin", which gRPC uses to mark binary metadata`},
			},
		},
		{
			name:  "only the suffix is binary, not the substring",
			label: config.MetricLabel{Header: "x-robin", Name: "tenant"},
		},
		{
			// Two independent problems, so unlike an empty value this is
			// reported twice rather than once.
			name:  "reserved and binary aggregate on one header",
			label: config.MetricLabel{Header: "grpc-trace-bin", Name: "tenant"},
			wantErrs: []validation.Error{
				{Field: "header", Message: `must not begin with "grpc-", which gRPC reserves`},
				{Field: "header", Message: `must not end with "-bin", which gRPC uses to mark binary metadata`},
			},
		},
		{
			// Prometheus reserves the "__" prefix and refuses to register a
			// collector using one, so it fails here rather than at runtime.
			name:  "a reserved label name",
			label: config.MetricLabel{Header: "x-h", Name: "__name__"},
			wantErrs: []validation.Error{
				{Field: "name", Message: `must not begin with "__", which Prometheus reserves`},
			},
		},
		{
			// A histogram panics on "le" when it first instantiates a series,
			// which is while serving a request, so config has to refuse it.
			name:  "a label named le",
			label: config.MetricLabel{Header: "x-h", Name: "le"},
			wantErrs: []validation.Error{
				{Field: "name", Message: `must not be "le", which Prometheus reserves`},
			},
		},
		{
			name:  "a label named quantile",
			label: config.MetricLabel{Header: "x-h", Name: "quantile"},
			wantErrs: []validation.Error{
				{Field: "name", Message: `must not be "quantile", which Prometheus reserves`},
			},
		},
		{
			name:  "a label merely containing a reserved name is allowed",
			label: config.MetricLabel{Header: "x-h", Name: "sample_quantile"},
		},
		{
			name:  "a bare reserved prefix",
			label: config.MetricLabel{Header: "x-h", Name: "__"},
			wantErrs: []validation.Error{
				{Field: "name", Message: `must not begin with "__", which Prometheus reserves`},
			},
		},
		{
			name:  "failures on both halves aggregate",
			label: config.MetricLabel{},
			wantErrs: []validation.Error{
				{Field: "header", Message: "is required"},
				{Field: "name", Message: "is required"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := tt.label.Validate()
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

func TestLoad_MetricLabels(t *testing.T) {
	t.Parallel()

	t.Run("scalar metadata labels decode into header and name", func(t *testing.T) {
		t.Parallel()

		got, err := config.Load(strings.NewReader(
			"metrics:\n  labels:\n    metadata:\n      - \"x-tenant:tenant\"\n      - \" x-Region : region \"\n",
		))
		require.NoError(t, err)
		require.Equal(t, []config.MetricLabel{
			{Header: "x-tenant", Name: "tenant"},
			{Header: "x-Region", Name: "region"},
		}, got.Metrics.Labels.Metadata)
	})

	t.Run("a fixed map decodes from yaml", func(t *testing.T) {
		t.Parallel()

		got, err := config.Load(strings.NewReader(
			"metrics:\n  labels:\n    fixed:\n      region: us-west-2\n      zone: us-west-2a\n",
		))
		require.NoError(t, err)
		require.Equal(
			t,
			map[string]string{"region": "us-west-2", "zone": "us-west-2a"},
			got.Metrics.Labels.Fixed,
		)
	})

	t.Run("an absent metadata key leaves none configured", func(t *testing.T) {
		t.Parallel()

		got, err := config.Load(strings.NewReader("metrics:\n  namespace: acme\n"))
		require.NoError(t, err)
		require.Empty(t, got.Metrics.Labels.Metadata)
	})

	t.Run("a metadata label missing its colon fails the load", func(t *testing.T) {
		t.Parallel()

		// The unmarshaler rejects it, so this never reaches Validate.
		_, err := config.Load(strings.NewReader("metrics:\n  labels:\n    metadata:\n      - nocolon\n"))
		require.ErrorContains(t, err, "invalid metric label")
	})
}
