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
			name: "distinct tags",
			cfg: &config.Metrics{
				HostPort:  ":9090",
				Namespace: "tmprl_proxy",
				Tags: []config.MetricTag{
					{Header: "x-tenant", Label: "tenant"},
					{Header: "x-region", Label: "region"},
				},
			},
		},
		{
			// Prometheus refuses to register a collector with a duplicate label
			// name, so two headers may not share one label.
			name: "two tags sharing a label",
			cfg: &config.Metrics{
				HostPort:  ":9090",
				Namespace: "tmprl_proxy",
				Tags: []config.MetricTag{
					{Header: "x-a", Label: "dup"},
					{Header: "x-b", Label: "dup"},
				},
			},
			wantErrs: []validation.Error{
				{Field: "tags[label]", Message: "contains duplicate value: dup"},
			},
		},
		{
			name: "the same header twice under different labels is allowed",
			cfg: &config.Metrics{
				HostPort:  ":9090",
				Namespace: "tmprl_proxy",
				Tags: []config.MetricTag{
					{Header: "x-a", Label: "one"},
					{Header: "x-a", Label: "two"},
				},
			},
		},
		{
			name: "a broken tag carries its index as the subject",
			cfg: &config.Metrics{
				HostPort:  ":9090",
				Namespace: "tmprl_proxy",
				Tags: []config.MetricTag{
					{Header: "x-ok", Label: "ok"},
					{Header: "x-bad", Label: "not a label"},
				},
			},
			wantErrs: []validation.Error{{
				Subject: "tags[1]",
				Field:   "label",
				Message: `is not valid, must match: "^[a-zA-Z_][a-zA-Z0-9_]*$"`,
			}},
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

func TestParseMetricTag(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		in      string
		want    config.MetricTag
		wantErr bool
	}{
		{
			name: "header and label",
			in:   "x-tenant:tenant",
			want: config.MetricTag{Header: "x-tenant", Label: "tenant"},
		},
		{
			name: "whitespace around each half is trimmed",
			in:   "  x-tenant : tenant  ",
			want: config.MetricTag{Header: "x-tenant", Label: "tenant"},
		},
		{
			// Cut splits on the first colon, so the rest lands in the label and
			// is rejected by Validate rather than here.
			name: "splits on the first colon only",
			in:   "x-h:a:b",
			want: config.MetricTag{Header: "x-h", Label: "a:b"},
		},
		{
			// Parsing accepts an empty half; Validate is what requires both.
			name: "empty header",
			in:   ":tenant",
			want: config.MetricTag{Label: "tenant"},
		},
		{
			name: "empty label",
			in:   "x-tenant:",
			want: config.MetricTag{Header: "x-tenant"},
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

			got, err := config.ParseMetricTag(tt.in)
			if tt.wantErr {
				require.ErrorContains(t, err, "invalid metric tag")
				return
			}

			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestMetricTag_Validate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		tag      config.MetricTag
		wantErrs []validation.Error
	}{
		{
			name: "a metadata key and a label name",
			tag:  config.MetricTag{Header: "x-tenant", Label: "tenant"},
		},
		{
			name: "dots, dashes, and mixed case in the header",
			tag:  config.MetricTag{Header: "X-Acme.Tenant-ID", Label: "tenant"},
		},
		{
			name: "a single leading underscore is a legal label",
			tag:  config.MetricTag{Header: "x-h", Label: "_tenant"},
		},
		{
			// Reported once: match yields nothing for an empty value so the
			// Required check on the same field owns that case.
			name: "empty header reports only that it is required",
			tag:  config.MetricTag{Label: "tenant"},
			wantErrs: []validation.Error{
				{Field: "header", Message: "is required"},
			},
		},
		{
			name: "empty label reports only that it is required",
			tag:  config.MetricTag{Header: "x-tenant"},
			wantErrs: []validation.Error{
				{Field: "label", Message: "is required"},
			},
		},
		{
			name: "a space is not legal in a metadata key",
			tag:  config.MetricTag{Header: "bad header", Label: "tenant"},
			wantErrs: []validation.Error{
				{Field: "header", Message: `is not valid, must match: "^[a-zA-Z0-9._-]+$"`},
			},
		},
		{
			name: "a dash is not legal in a label name",
			tag:  config.MetricTag{Header: "x-h", Label: "not-a-label"},
			wantErrs: []validation.Error{
				{Field: "label", Message: `is not valid, must match: "^[a-zA-Z_][a-zA-Z0-9_]*$"`},
			},
		},
		{
			name: "a label may not start with a digit",
			tag:  config.MetricTag{Header: "x-h", Label: "1tenant"},
			wantErrs: []validation.Error{
				{Field: "label", Message: `is not valid, must match: "^[a-zA-Z_][a-zA-Z0-9_]*$"`},
			},
		},
		{
			// gRPC reserves the "grpc-" prefix for its own metadata.
			name: "a reserved header",
			tag:  config.MetricTag{Header: "grpc-status", Label: "tenant"},
			wantErrs: []validation.Error{
				{Field: "header", Message: `must not begin with "grpc-", which gRPC reserves`},
			},
		},
		{
			// Metadata keys are case-insensitive, so the prefix check must be
			// too or this spelling would slip through.
			name: "a reserved header in mixed case",
			tag:  config.MetricTag{Header: "GRPC-Status", Label: "tenant"},
			wantErrs: []validation.Error{
				{Field: "header", Message: `must not begin with "grpc-", which gRPC reserves`},
			},
		},
		{
			name: "only the prefix is reserved, not the substring",
			tag:  config.MetricTag{Header: "x-grpc-tenant", Label: "tenant"},
		},
		{
			name: "a header merely starting with grpc is allowed",
			tag:  config.MetricTag{Header: "grpcfoo", Label: "tenant"},
		},
		{
			// gRPC marks a binary value with the "-bin" suffix, and binary is
			// not valid UTF-8, so it cannot be a label value.
			name: "a binary metadata key",
			tag:  config.MetricTag{Header: "x-trace-bin", Label: "tenant"},
			wantErrs: []validation.Error{
				{Field: "header", Message: `must not end with "-bin", which gRPC uses to mark binary metadata`},
			},
		},
		{
			name: "a binary metadata key in mixed case",
			tag:  config.MetricTag{Header: "X-Trace-BIN", Label: "tenant"},
			wantErrs: []validation.Error{
				{Field: "header", Message: `must not end with "-bin", which gRPC uses to mark binary metadata`},
			},
		},
		{
			name: "only the suffix is binary, not the substring",
			tag:  config.MetricTag{Header: "x-robin", Label: "tenant"},
		},
		{
			// Two independent problems, so unlike an empty value this is
			// reported twice rather than once.
			name: "reserved and binary aggregate on one header",
			tag:  config.MetricTag{Header: "grpc-trace-bin", Label: "tenant"},
			wantErrs: []validation.Error{
				{Field: "header", Message: `must not begin with "grpc-", which gRPC reserves`},
				{Field: "header", Message: `must not end with "-bin", which gRPC uses to mark binary metadata`},
			},
		},
		{
			// Prometheus reserves the "__" prefix and refuses to register a
			// collector using one, so it fails here rather than at runtime.
			name: "a reserved label name",
			tag:  config.MetricTag{Header: "x-h", Label: "__name__"},
			wantErrs: []validation.Error{
				{Field: "label", Message: `must not begin with "__", which Prometheus reserves`},
			},
		},
		{
			// A histogram panics on "le" when it first instantiates a series,
			// which is while serving a request, so config has to refuse it.
			name: "a label named le",
			tag:  config.MetricTag{Header: "x-h", Label: "le"},
			wantErrs: []validation.Error{
				{Field: "label", Message: `must not be "le", which Prometheus reserves`},
			},
		},
		{
			name: "a label named quantile",
			tag:  config.MetricTag{Header: "x-h", Label: "quantile"},
			wantErrs: []validation.Error{
				{Field: "label", Message: `must not be "quantile", which Prometheus reserves`},
			},
		},
		{
			name: "a label merely containing a reserved name is allowed",
			tag:  config.MetricTag{Header: "x-h", Label: "sample_quantile"},
		},
		{
			name: "a bare reserved prefix",
			tag:  config.MetricTag{Header: "x-h", Label: "__"},
			wantErrs: []validation.Error{
				{Field: "label", Message: `must not begin with "__", which Prometheus reserves`},
			},
		},
		{
			name: "failures on both halves aggregate",
			tag:  config.MetricTag{},
			wantErrs: []validation.Error{
				{Field: "header", Message: "is required"},
				{Field: "label", Message: "is required"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := tt.tag.Validate()
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

func TestLoad_MetricTags(t *testing.T) {
	t.Parallel()

	t.Run("scalar tags decode into header and label", func(t *testing.T) {
		t.Parallel()

		got, err := config.Load(strings.NewReader(
			"metrics:\n  tags:\n    - \"x-tenant:tenant\"\n    - \" x-Region : region \"\n",
		))
		require.NoError(t, err)
		require.Equal(t, []config.MetricTag{
			{Header: "x-tenant", Label: "tenant"},
			{Header: "x-Region", Label: "region"},
		}, got.Metrics.Tags)
	})

	t.Run("an absent tags key leaves no tags", func(t *testing.T) {
		t.Parallel()

		got, err := config.Load(strings.NewReader("metrics:\n  namespace: acme\n"))
		require.NoError(t, err)
		require.Empty(t, got.Metrics.Tags)
	})

	t.Run("a tag missing its colon fails the load", func(t *testing.T) {
		t.Parallel()

		// The unmarshaler rejects it, so this never reaches Validate.
		_, err := config.Load(strings.NewReader("metrics:\n  tags:\n    - nocolon\n"))
		require.ErrorContains(t, err, "invalid metric tag")
	})
}
