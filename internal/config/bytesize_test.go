package config_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/temporalio/temporal-proxy/internal/config"
)

func TestParseByteSize(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		in      string
		want    config.ByteSize
		wantErr string
	}{
		{name: "bare integer is bytes", in: "1024", want: 1024},
		{name: "bytes suffix", in: "512B", want: 512},
		{name: "kibibytes", in: "4KiB", want: 4 << 10},
		{name: "mebibytes", in: "128MiB", want: 128 << 20},
		{name: "gibibytes", in: "1GiB", want: 1 << 30},
		{name: "space before the unit", in: "128 MiB", want: 128 << 20},
		{name: "surrounding whitespace", in: "  8MiB ", want: 8 << 20},
		{name: "kilobytes are decimal", in: "4KB", want: 4_000},
		{name: "megabytes are decimal", in: "128MB", want: 128_000_000},
		{name: "gigabytes are decimal", in: "1GB", want: 1_000_000_000},
		{name: "bare unit letters are rejected", in: "128M", wantErr: `unknown unit "M"`},
		{name: "unknown unit", in: "128XB", wantErr: `unknown unit "XB"`},
		{name: "unit is case sensitive", in: "128mib", wantErr: `unknown unit "mib"`},
		{name: "fractions are rejected", in: "1.5GiB", wantErr: "invalid byte size"},
		{name: "negative is rejected", in: "-1MiB", wantErr: "invalid byte size"},
		{name: "unit without a number", in: "MiB", wantErr: "invalid byte size"},
		{name: "empty string", in: "", wantErr: "invalid byte size"},
		{name: "overflow is rejected", in: "99999999999GiB", wantErr: "overflows"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := config.ParseByteSize(tt.in)
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)
				return
			}

			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestByteSize_String(t *testing.T) {
	t.Parallel()

	tests := []struct {
		in   config.ByteSize
		want string
	}{
		{in: 0, want: "0B"},
		{in: 1023, want: "1023B"},
		{in: 2 << 30, want: "2GiB"},
		{in: 128 << 20, want: "128MiB"},
		{in: 128_000_000, want: "128MB"},
		{in: 4_000, want: "4KB"},
	}

	for _, tt := range tests {
		t.Run(tt.want, func(t *testing.T) {
			t.Parallel()

			require.Equal(t, tt.want, tt.in.String())
		})
	}
}
