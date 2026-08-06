package proxy

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/metadata"
)

func TestForwardContext(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		incoming     metadata.MD
		outgoing     metadata.MD
		wantOutgoing metadata.MD
		wantOK       bool
	}{
		{
			name: "strips all three transport headers",
			incoming: metadata.MD{
				"user-agent":   {"caller-agent"},
				":authority":   {"caller-host"},
				"content-type": {"application/grpc"},
				"x-custom":     {"kept"},
			},
			wantOutgoing: metadata.MD{"x-custom": {"kept"}},
			wantOK:       true,
		},
		{
			name:         "does not overwrite an existing outgoing value",
			incoming:     metadata.MD{"x-custom": {"from-caller"}},
			outgoing:     metadata.MD{"x-custom": {"already-set"}},
			wantOutgoing: metadata.MD{"x-custom": {"already-set"}},
			wantOK:       true,
		},
		{
			name:         "copies other incoming keys through",
			incoming:     metadata.MD{"x-a": {"1"}, "x-b": {"2"}},
			wantOutgoing: metadata.MD{"x-a": {"1"}, "x-b": {"2"}},
			wantOK:       true,
		},
		{
			name:   "no incoming metadata leaves the outgoing context untouched",
			wantOK: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			if tt.incoming != nil {
				ctx = metadata.NewIncomingContext(ctx, tt.incoming)
			}
			if tt.outgoing != nil {
				ctx = metadata.NewOutgoingContext(ctx, tt.outgoing)
			}

			got, ok := metadata.FromOutgoingContext(forwardContext(ctx))
			require.Equal(t, tt.wantOK, ok)
			require.Equal(t, tt.wantOutgoing, got)
		})
	}
}
