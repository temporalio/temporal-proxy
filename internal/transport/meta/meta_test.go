package meta_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/metadata"

	"github.com/temporalio/temporal-proxy/internal/transport/meta"
)

func TestWithNamespaceRoundTrips(t *testing.T) {
	t.Parallel()

	ctx := meta.WithNamespace(t.Context(), "orders")
	require.Equal(t, "orders", meta.NamespaceFrom(ctx))
}

func TestWithNamespaceOverwritesExisting(t *testing.T) {
	t.Parallel()

	// A spoofed value already present in outgoing metadata is replaced.
	ctx := metadata.NewOutgoingContext(t.Context(), metadata.Pairs(meta.NamespaceHeader, "spoofed"))
	ctx = meta.WithNamespace(ctx, "orders")

	md, _ := metadata.FromOutgoingContext(ctx)
	require.Equal(t, []string{"orders"}, md.Get(meta.NamespaceHeader))
}

func TestNamespaceFromReturnsLastValue(t *testing.T) {
	t.Parallel()

	ctx := metadata.NewOutgoingContext(t.Context(), metadata.Pairs(
		meta.NamespaceHeader, "old",
		meta.NamespaceHeader, "new",
	))
	require.Equal(t, "new", meta.NamespaceFrom(ctx))
}

func TestNamespaceFromAbsentIsEmpty(t *testing.T) {
	t.Parallel()

	require.Equal(t, "", meta.NamespaceFrom(t.Context()))
}

func TestWithTargetRoundTrips(t *testing.T) {
	t.Parallel()

	target := meta.Target{FullName: "/temporal.api.workflowservice.v1.WorkflowService/StartWorkflowExecution", Namespace: "orders"}

	ctx := meta.WithTarget(t.Context(), target)
	require.Equal(t, target, meta.TargetFrom(ctx))
}

func TestTargetFromAbsentIsZero(t *testing.T) {
	t.Parallel()

	require.Equal(t, meta.Target{}, meta.TargetFrom(t.Context()))
}

func TestTargetFromIgnoresMetadata(t *testing.T) {
	t.Parallel()

	// A caller cannot forge a Target by sending metadata: it is carried as a
	// context value the gateway sets, and authentication decides on it.
	ctx := metadata.NewIncomingContext(t.Context(), metadata.Pairs(meta.NamespaceHeader, "spoofed"))
	require.Equal(t, meta.Target{}, meta.TargetFrom(ctx))
}

func TestWithVersionSetsTheHeader(t *testing.T) {
	t.Parallel()

	ctx := meta.WithVersion(t.Context(), "1.4.2")

	md, _ := metadata.FromOutgoingContext(ctx)
	require.Equal(t, []string{"1.4.2"}, md.Get(meta.VersionHeader))
}

func TestWithVersionReplacesAClientSuppliedValue(t *testing.T) {
	t.Parallel()

	// The forwarder relays the caller's inbound metadata onward, so a client
	// sending the header itself must not reach the upstream alongside ours.
	ctx := metadata.NewOutgoingContext(t.Context(), metadata.Pairs(meta.VersionHeader, "spoofed"))
	ctx = meta.WithVersion(ctx, "1.4.2")

	md, _ := metadata.FromOutgoingContext(ctx)
	require.Equal(t, []string{"1.4.2"}, md.Get(meta.VersionHeader))
}

func TestWithVersionKeepsOtherMetadata(t *testing.T) {
	t.Parallel()

	ctx := meta.WithNamespace(t.Context(), "orders")
	ctx = meta.WithVersion(ctx, "1.4.2")

	require.Equal(t, "orders", meta.NamespaceFrom(ctx))
}

func TestHTTPGroups(t *testing.T) {
	t.Parallel()

	groups := meta.HTTPGroups()
	require.NotEmpty(t, groups)
	require.NotContains(t, groups, meta.HTTPGroupUnspecified)

	seen := map[string]bool{}
	for _, g := range groups {
		name := g.String()
		require.NotEqual(t, meta.HTTPGroupUnspecified.String(), name, "group %d needs a String case", int(g))
		require.NotContains(t, name, "HTTPGroup(", "group %d needs a String case", int(g))
		require.False(t, seen[name], "duplicate group name %q", name)
		seen[name] = true
	}
}

func TestHTTPGroupString(t *testing.T) {
	t.Parallel()

	tests := []struct {
		group meta.HTTPGroup
		want  string
	}{
		{group: meta.HTTPGroupUnspecified, want: "unspecified"},
		{group: meta.HTTPGroupCodecServer, want: "codecServer"},
		{group: meta.HTTPGroup(99), want: "HTTPGroup(99)"},
	}

	for _, tt := range tests {
		t.Run(tt.want, func(t *testing.T) {
			t.Parallel()

			require.Equal(t, tt.want, tt.group.String())
		})
	}
}
