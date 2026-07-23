package validation_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/temporalio/temporal-proxy/pkg/validation"
)

type sampleStruct struct {
	Name string
	Age  int
}

func TestRequired_String(t *testing.T) {
	t.Parallel()

	check := validation.Required[string]()
	require.NoError(t, check("foo"))
	require.Error(t, check(""))
}

func TestRequired_Int(t *testing.T) {
	t.Parallel()

	check := validation.Required[int]()
	require.NoError(t, check(42))
	require.NoError(t, check(-1))
	require.Error(t, check(0))
}

func TestRequired_Pointer(t *testing.T) {
	t.Parallel()

	check := validation.Required[*string]()
	s := "x"
	require.NoError(t, check(&s))
	require.Error(t, check(nil))
}

func TestRequired_Struct(t *testing.T) {
	t.Parallel()

	check := validation.Required[sampleStruct]()
	require.NoError(t, check(sampleStruct{Name: "bob"}))
	require.Error(t, check(sampleStruct{}))
}

func TestRequired_TypeInferenceWithField(t *testing.T) {
	t.Parallel()

	// Sanity check that Required composes with Field. Note: Required must be
	// explicitly instantiated (Required[string]()) because Go's type inference
	// does not propagate Field's V through the Check[V] return type.
	rule := validation.Field("name", "", validation.Required[string]())
	errs := rule()
	require.Len(t, errs, 1)
}

func TestUnique(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name    string
		input   []string
		wantErr bool
	}{
		{name: "empty slice", input: nil, wantErr: false},
		{name: "single element", input: []string{"a"}, wantErr: false},
		{name: "all distinct", input: []string{"a", "b", "c"}, wantErr: false},
		{name: "one duplicate", input: []string{"a", "b", "a"}, wantErr: true},
		{name: "multiple duplicates", input: []string{"a", "a", "b", "b"}, wantErr: true},
	}

	check := validation.Unique[string]()
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			err := check(tc.input)
			if tc.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestUnique_Int(t *testing.T) {
	t.Parallel()

	check := validation.Unique[int]()
	require.NoError(t, check([]int{1, 2, 3}))
	require.Error(t, check([]int{1, 2, 2}))
}

func TestUnique_InferredFromField(t *testing.T) {
	t.Parallel()

	// Sanity check that Unique composes with Field. Note: Unique must be
	// explicitly instantiated (Unique[string]()) because Go's type inference
	// does not propagate Field's V through the Check[V] return type.
	rule := validation.Field("tags", []string{"a", "a"}, validation.Unique[string]())
	errs := rule()
	require.Len(t, errs, 1)
}

func TestGT(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		mark    int
		in      int
		wantErr bool
	}{
		{name: "greater", mark: 0, in: 1},
		{name: "much greater", mark: 10, in: 1000},
		{name: "negative bound", mark: -5, in: -1},
		{name: "equal is rejected", mark: 0, in: 0, wantErr: true},
		{name: "less is rejected", mark: 0, in: -1, wantErr: true},
	}

	check := validation.GT(0)
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := validation.GT(tt.mark)(tt.in)
			if tt.wantErr {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)
		})
	}

	// Sanity: the check instantiated once above still rejects its bound.
	require.Error(t, check(0))
}

func TestGT_OrderedTypes(t *testing.T) {
	t.Parallel()

	require.NoError(t, validation.GT(1.5)(1.6))
	require.Error(t, validation.GT(1.5)(1.5))
	require.NoError(t, validation.GT("abc")("abd"))
	require.Error(t, validation.GT("abc")("abc"))
}

func TestGTE(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		mark    int
		in      int
		wantErr bool
	}{
		{name: "greater", mark: 0, in: 1},
		{name: "equal is accepted", mark: 0, in: 0},
		{name: "negative bound equal", mark: -5, in: -5},
		{name: "less is rejected", mark: 0, in: -1, wantErr: true},
		{name: "just below is rejected", mark: 10, in: 9, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := validation.GTE(tt.mark)(tt.in)
			if tt.wantErr {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)
		})
	}
}

func TestGTE_OrderedTypes(t *testing.T) {
	t.Parallel()

	require.NoError(t, validation.GTE(1.5)(1.5))
	require.Error(t, validation.GTE(1.5)(1.4))
	require.NoError(t, validation.GTE("abc")("abc"))
	require.Error(t, validation.GTE("abc")("abb"))
}

func TestLT(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		mark    int
		in      int
		wantErr bool
	}{
		{name: "less", mark: 0, in: -1},
		{name: "much less", mark: 1000, in: 10},
		{name: "negative bound", mark: -5, in: -10},
		{name: "equal is rejected", mark: 0, in: 0, wantErr: true},
		{name: "greater is rejected", mark: 0, in: 1, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := validation.LT(tt.mark)(tt.in)
			if tt.wantErr {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)
		})
	}
}

func TestLT_OrderedTypes(t *testing.T) {
	t.Parallel()

	require.NoError(t, validation.LT(1.5)(1.4))
	require.Error(t, validation.LT(1.5)(1.5))
	require.NoError(t, validation.LT("abc")("abb"))
	require.Error(t, validation.LT("abc")("abc"))
}

func TestIsHostPort(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		in      string
		wantErr bool
	}{
		{name: "host and port", in: "localhost:8080"},
		{name: "ipv4 and port", in: "127.0.0.1:8080"},
		{name: "ipv6 and port", in: "[::1]:8080"},
		{name: "wildcard host", in: ":8080"},
		{name: "wildcard port zero", in: ":0"},
		// Reserved TLD per RFC 2606; passing this proves we no longer do DNS lookup.
		{name: "non-resolvable hostname is syntactically valid", in: "host.invalid:8080"},
		{name: "missing port", in: "localhost", wantErr: true},
		{name: "empty port after colon", in: "localhost:", wantErr: true},
		{name: "non-numeric port", in: "localhost:http", wantErr: true},
		{name: "port out of range", in: "localhost:70000", wantErr: true},
		{name: "negative port", in: "localhost:-1", wantErr: true},
		{name: "url with scheme", in: "https://example.com:8080", wantErr: true},
	}

	check := validation.IsHostPort()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := check(tt.in)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
		})
	}
}
