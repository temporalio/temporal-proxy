package resolve_test

import (
	"net/url"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/resolver"

	"github.com/temporalio/temporal-proxy/internal/transport/resolve"
)

func TestIsTarget(t *testing.T) {
	t.Parallel()

	tests := []struct {
		target string
		want   bool
	}{
		{"srv:///_grpc._tcp.frontend.example", true},
		{"srv://frontend.example", true},
		{"localhost:7233", false},
		{"dns:///frontend.example:7233", false},
		{"{{ .RemoteNamespace }}.example:7233", false},
	}

	for _, tt := range tests {
		t.Run(tt.target, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tt.want, resolve.IsTarget(tt.target))
		})
	}
}

func TestParseTarget(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		target  string
		want    string
		wantErr string
	}{
		{
			name:   "full record name",
			target: "srv:///_grpc._tcp.frontend.temporal.svc",
			want:   "_grpc._tcp.frontend.temporal.svc",
		},
		{
			name:   "trailing dot is kept for the lookup",
			target: "srv:///_grpc._tcp.frontend.example.",
			want:   "_grpc._tcp.frontend.example.",
		},
		{
			name:    "two slashes put the name in the host",
			target:  "srv://frontend.example",
			wantErr: "use srv:///<name>",
		},
		{
			name:    "opaque form is rejected",
			target:  "srv:frontend.example",
			wantErr: "use srv:///<name>",
		},
		{
			name:    "host named srv is not a record",
			target:  "srv:7233",
			wantErr: "use srv:///<name>",
		},
		{
			name:    "port is rejected",
			target:  "srv:///frontend.example:7233",
			wantErr: "ports come from the records",
		},
		{
			name:    "path is rejected",
			target:  "srv:///frontend.example/extra",
			wantErr: "ports come from the records",
		},
		{
			name:    "empty name",
			target:  "srv:///",
			wantErr: "missing SRV record name",
		},
		{
			name:    "wrong scheme",
			target:  "dns:///frontend.example",
			wantErr: `scheme must be "srv"`,
		},
		{
			name:    "unparseable",
			target:  "srv:///%zz",
			wantErr: "invalid URL escape",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := resolve.ParseTarget(tt.target)
			if tt.wantErr != "" {
				require.ErrorIs(t, err, resolve.ErrInvalidTarget)
				require.ErrorContains(t, err, tt.wantErr)
				return
			}

			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestBuilderScheme(t *testing.T) {
	t.Parallel()
	require.Equal(t, "srv", resolve.NewSRVBuilder().Scheme())
}

func TestBuilderOverrideAuthority(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		target string
		want   string
	}{
		{"service and proto labels stripped", "srv:///_grpc._tcp.frontend.temporal.svc", "frontend.temporal.svc"},
		{"trailing dot dropped", "srv:///_grpc._tcp.frontend.example.", "frontend.example"},
		{"no underscore labels", "srv:///frontend.example", "frontend.example"},
		{"only underscore labels left alone", "srv:///_grpc._tcp", "_grpc._tcp"},
		{"underscore inside a later label kept", "srv:///_grpc._tcp.my_host.example", "my_host.example"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tt.want, resolve.NewSRVBuilder().OverrideAuthority(mustTarget(t, tt.target)))
		})
	}
}

// mustTarget parses s the way grpc.NewClient does before handing it to a builder.
func mustTarget(t *testing.T, s string) resolver.Target {
	t.Helper()

	u, err := url.Parse(s)
	require.NoError(t, err)

	return resolver.Target{URL: *u}
}
