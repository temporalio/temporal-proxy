package template_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/temporalio/temporal-proxy/internal/template"
)

func TestParseUpstream_Render(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		tmpl string
		ctx  template.UpstreamContext
		want string
	}{
		{
			name: "static string round-trips",
			tmpl: "localhost:7233",
			want: "localhost:7233",
		},
		{
			name: "remote namespace in hostPort",
			tmpl: "{{ .RemoteNamespace }}.acme-cloud.tmprl.cloud:7233",
			ctx:  template.UpstreamContext{RemoteNamespace: "payments"},
			want: "payments.acme-cloud.tmprl.cloud:7233",
		},
		{
			name: "remote namespace in serverName",
			tmpl: "{{ .RemoteNamespace }}.aws.tmprl.cloud",
			ctx:  template.UpstreamContext{RemoteNamespace: "payments"},
			want: "payments.aws.tmprl.cloud",
		},
		{
			name: "local namespace",
			tmpl: "{{ .LocalNamespace }}",
			ctx:  template.UpstreamContext{LocalNamespace: "orders"},
			want: "orders",
		},
		{
			name: "metadata dot form",
			tmpl: "{{ .Metadata.dc }}.internal",
			ctx:  template.UpstreamContext{Metadata: map[string]string{"dc": "west"}},
			want: "west.internal",
		},
		{
			name: "metadata index form",
			tmpl: `{{ index .Metadata "x-cluster" }}`,
			ctx:  template.UpstreamContext{Metadata: map[string]string{"x-cluster": "c1"}},
			want: "c1",
		},
		{
			name: "absent metadata key renders empty",
			tmpl: "{{ .Metadata.dc }}.internal",
			ctx:  template.UpstreamContext{Metadata: map[string]string{}},
			want: ".internal",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			tmpl, err := template.ParseUpstream(tt.tmpl)
			require.NoError(t, err)
			require.Equal(t, tt.tmpl, tmpl.String())

			got, err := tmpl.Render(tt.ctx)
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestParseRouting_Render(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		tmpl string
		ctx  template.RoutingContext
		want string
	}{
		{
			name: "local namespace",
			tmpl: "{{ .LocalNamespace }}",
			ctx:  template.RoutingContext{LocalNamespace: "orders"},
			want: "orders",
		},
		{
			name: "metadata index form",
			tmpl: `{{ index .Metadata "x-cluster" }}`,
			ctx:  template.RoutingContext{Metadata: map[string]string{"x-cluster": "c1"}},
			want: "c1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			tmpl, err := template.ParseRouting(tt.tmpl)
			require.NoError(t, err)

			got, err := tmpl.Render(tt.ctx)
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}

// A routing template cannot reference RemoteNamespace (it is not known until an
// upstream is selected); the same template is valid for an upstream.
func TestRemoteNamespace_scopedToUpstream(t *testing.T) {
	t.Parallel()

	const tmpl = "{{ .RemoteNamespace }}.acme-cloud.tmprl.cloud:7233"

	_, err := template.ParseRouting(tmpl)
	require.Error(t, err)
	require.Contains(t, err.Error(), "RemoteNamespace")

	_, err = template.ParseUpstream(tmpl)
	require.NoError(t, err)
}

func TestParse_errors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		tmpl string
	}{
		{name: "malformed syntax", tmpl: "{{ .LocalNamespace"},
		{name: "unknown field", tmpl: "{{ .Foo }}.acme.com"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, rErr := template.ParseRouting(tt.tmpl)
			require.Error(t, rErr)

			_, uErr := template.ParseUpstream(tt.tmpl)
			require.Error(t, uErr)
		})
	}
}

func TestMust(t *testing.T) {
	t.Parallel()

	require.NotPanics(t, func() { template.Must(template.ParseUpstream("{{ .RemoteNamespace }}.acme.com")) })
	require.NotPanics(t, func() { template.Must(template.ParseRouting("{{ .LocalNamespace }}")) })
	require.Panics(t, func() { template.Must(template.ParseRouting("{{ .RemoteNamespace }}")) })
	require.Panics(t, func() { template.Must(template.ParseUpstream("{{ .Foo }}")) })
}
