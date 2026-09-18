package ext_test

import (
	"math"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/temporalio/temporal-proxy/pkg/ext"
)

// TestBindingContextEncoding pins the bytes. They are permanent: every payload
// already sealed authenticates against them, and a [ext.KeySealer] that passed
// them to its key service cannot open what it sealed if they move.
func TestBindingContextEncoding(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		name      string
		namespace string
		version   string
		opaque    []byte
		want      []byte
	}{
		{
			name:      "namespace and version",
			namespace: "orders",
			version:   "1",
			want: []byte{
				0x00, 0x00, 0x00, 0x00, // cipher, always unset here
				0x00, 0x01, '1',
				0x00, 0x06, 'o', 'r', 'd', 'e', 'r', 's',
				0x00, 0x00, 0x00, 0x00, // no opaque
			},
		},
		{
			name: "nothing at all",
			want: []byte{
				0x00, 0x00, 0x00, 0x00,
				0x00, 0x00,
				0x00, 0x00,
				0x00, 0x00, 0x00, 0x00,
			},
		},
		{
			name:   "opaque only",
			opaque: []byte{0xde, 0xad},
			want: []byte{
				0x00, 0x00, 0x00, 0x00,
				0x00, 0x00,
				0x00, 0x00,
				0x00, 0x00, 0x00, 0x02, 0xde, 0xad,
			},
		},
		{
			// The prefixes count bytes, not runes.
			name:      "a multi-byte namespace",
			namespace: "née",
			want: []byte{
				0x00, 0x00, 0x00, 0x00,
				0x00, 0x00,
				0x00, 0x04, 0x6e, 0xc3, 0xa9, 0x65,
				0x00, 0x00, 0x00, 0x00,
			},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := ext.BindingContext(tt.namespace, tt.version, tt.opaque)
			require.NoError(t, err)
			require.Equal(t, tt.want, got)

			// Unwrap refuses material naming a cipher precisely because this
			// encoding always claims none. That dependency is invisible otherwise.
			require.Equal(t, []byte{0, 0, 0, 0}, got[:4], "the cipher is always unset")
		})
	}
}

// TestBindingContextIsUnambiguous is what fails loudest if someone ever
// "simplifies" the length prefixes away. Each pair concatenates to the same
// bytes and must not bind to the same context, or material could be relabelled
// across the boundary between two fields and still open.
func TestBindingContextIsUnambiguous(t *testing.T) {
	t.Parallel()

	type fields struct {
		namespace string
		version   string
		opaque    []byte
	}

	for _, tt := range []struct {
		name string
		a, b fields
	}{
		{
			name: "across namespace and version",
			a:    fields{namespace: "ab"},
			b:    fields{namespace: "b", version: "a"},
		},
		{
			name: "across namespace and opaque",
			a:    fields{namespace: "a", opaque: []byte("bc")},
			b:    fields{namespace: "ab", opaque: []byte("c")},
		},
		{
			name: "an empty field is not no field",
			a:    fields{namespace: "ab"},
			b:    fields{version: "ab"},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			a, err := ext.BindingContext(tt.a.namespace, tt.a.version, tt.a.opaque)
			require.NoError(t, err)

			b, err := ext.BindingContext(tt.b.namespace, tt.b.version, tt.b.opaque)
			require.NoError(t, err)

			require.NotEqual(t, a, b)
		})
	}
}

// TestBindingContextRejectsUnframeableFields pins the attribution split, which
// looks like an inconsistency and is not: in [ext.NewKeyWrapper] the namespace
// comes from the caller and the version from the key store, so only one of them
// is the caller's fault. A later tidy-up would change the code the proxy sees.
func TestBindingContextRejectsUnframeableFields(t *testing.T) {
	t.Parallel()

	tooLong := strings.Repeat("x", math.MaxUint16+1)

	t.Run("the version is the key service's fault", func(t *testing.T) {
		t.Parallel()

		_, err := ext.BindingContext("orders", tooLong, nil)
		require.ErrorContains(t, err, "key version is too long")
		require.Equal(t, codes.Unknown, status.Code(err), "no code, so the handler picks one")
	})

	t.Run("the namespace is the caller's", func(t *testing.T) {
		t.Parallel()

		_, err := ext.BindingContext(tooLong, "1", nil)
		require.ErrorContains(t, err, "namespace is too long")
		require.Equal(t, codes.InvalidArgument, status.Code(err))
	})
}
