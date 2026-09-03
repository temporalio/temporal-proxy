package metrics_test

import (
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/metadata"

	"github.com/temporalio/temporal-proxy/internal/config"
	"github.com/temporalio/temporal-proxy/internal/metrics"
)

func TestNewTags(t *testing.T) {
	t.Parallel()

	t.Run("no tags yields the zero value", func(t *testing.T) {
		t.Parallel()

		tags := metrics.NewTags(nil)
		require.Zero(t, tags.Len())
		require.Empty(t, tags.Labels())
	})

	t.Run("labels keep configured order and case", func(t *testing.T) {
		t.Parallel()

		tags := metrics.NewTags([]config.MetricTag{
			{Header: "x-tenant", Label: "Tenant"},
			{Header: "x-region", Label: "region"},
		})

		require.Equal(t, 2, tags.Len())
		require.Equal(t, []string{"Tenant", "region"}, tags.Labels())
	})

	t.Run("Labels returns a copy", func(t *testing.T) {
		t.Parallel()

		tags := metrics.NewTags([]config.MetricTag{{Header: "x-tenant", Label: "tenant"}})

		got := tags.Labels()
		got[0] = "mutated"
		require.Equal(t, []string{"tenant"}, tags.Labels())
	})
}

func TestTags_AppendValues(t *testing.T) {
	t.Parallel()

	tags := metrics.NewTags([]config.MetricTag{
		{Header: "x-tenant", Label: "tenant"},
		{Header: "x-region", Label: "region"},
	})

	tests := []struct {
		name string
		md   metadata.MD // nil means no incoming metadata at all
		want []string
	}{
		{
			name: "both headers present",
			md:   metadata.Pairs("x-tenant", "acme", "x-region", "us-east"),
			want: []string{"acme", "us-east"},
		},
		{
			name: "values follow label order, not metadata order",
			md:   metadata.Pairs("x-region", "us-east", "x-tenant", "acme"),
			want: []string{"acme", "us-east"},
		},
		{
			// gRPC canonicalizes keys to lowercase, so a header the config wrote
			// in mixed case still has to match.
			name: "a mixed-case header on the wire still matches",
			md:   metadata.Pairs("X-Tenant", "acme"),
			want: []string{"acme", ""},
		},
		{
			name: "an absent header yields an empty value",
			md:   metadata.Pairs("x-tenant", "acme"),
			want: []string{"acme", ""},
		},
		{
			name: "no incoming metadata yields empty values",
			md:   nil,
			want: []string{"", ""},
		},
		{
			// Matches how the proxy reduces multi-valued metadata elsewhere, e.g.
			// the template context in internal/proxy.
			name: "the last of several values wins",
			md:   metadata.Pairs("x-tenant", "first", "x-tenant", "last"),
			want: []string{"last", ""},
		},
		{
			// client_golang panics on a label value that is not valid UTF-8, and
			// gRPC does not police the bytes of a textual header, so a caller
			// could otherwise crash the process on demand.
			name: "a value that is not valid UTF-8 is dropped",
			md:   metadata.Pairs("x-tenant", string([]byte{0xff, 0xfe}), "x-region", "us-east"),
			want: []string{"", "us-east"},
		},
		{
			// The guard applies to whichever value the last-wins rule picked, so
			// a bad value last is cleared rather than falling back to a good one
			// earlier in the list.
			name: "an earlier valid value does not rescue an invalid last one",
			md:   metadata.Pairs("x-tenant", "acme", "x-tenant", string([]byte{0xff})),
			want: []string{"", ""},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			if tt.md != nil {
				ctx = metadata.NewIncomingContext(ctx, tt.md)
			}

			require.Equal(t, tt.want, tags.AppendValues(ctx, nil))
		})
	}
}

func TestTags_AppendValuesExtendsTheCallersSlice(t *testing.T) {
	t.Parallel()

	tags := metrics.NewTags([]config.MetricTag{{Header: "x-tenant", Label: "tenant"}})
	ctx := metadata.NewIncomingContext(t.Context(), metadata.Pairs("x-tenant", "acme"))

	require.Equal(t, []string{"method", "code", "acme"}, tags.AppendValues(ctx, []string{"method", "code"}))
}

func TestTags_AppendValuesWithNoTagsLeavesTheSliceAlone(t *testing.T) {
	t.Parallel()

	// The fast path every deployment that configures no tags takes: the label
	// list is handed back exactly as it arrived.
	var tags metrics.Tags

	require.Equal(t, []string{"method"}, tags.AppendValues(t.Context(), []string{"method"}))
	require.Nil(t, tags.AppendValues(t.Context(), nil))
}

func TestTags_AppendValuesTruncatesOnARuneBoundary(t *testing.T) {
	t.Parallel()

	tags := metrics.NewTags([]config.MetricTag{{Header: "x-tenant", Label: "tenant"}})

	// Each euro sign is three bytes, so a cut at 256 bytes lands mid-rune and
	// the partial tail has to be dropped rather than reported.
	ctx := metadata.NewIncomingContext(t.Context(), metadata.Pairs("x-tenant", strings.Repeat("€", 200)))

	got := tags.AppendValues(ctx, nil)
	require.Len(t, got, 1)
	require.True(t, utf8.ValidString(got[0]), "a truncated value must still be valid UTF-8")
	require.Equal(t, strings.Repeat("€", 85), got[0], "expected a whole number of runes within the cap")
}
