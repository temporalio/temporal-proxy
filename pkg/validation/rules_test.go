package validation_test

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/temporalio/temporal-proxy/pkg/validation"
)

func TestField_NoChecks(t *testing.T) {
	t.Parallel()

	rule := validation.Field("name", "foo")
	require.Empty(t, rule())
}

func TestField_NormalizesCheckOutput(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		checkRet error
		want     validation.Errors
	}{
		{
			name:     "nil produces no entries",
			checkRet: nil,
			want:     nil,
		},
		{
			name:     "plain error is wrapped with Field name",
			checkRet: errors.New("boom"),
			want:     validation.Errors{{Field: "name", Message: "boom"}},
		},
		{
			name:     "validation.Error with empty Field gets stamped",
			checkRet: validation.Error{Message: "boom"},
			want:     validation.Errors{{Field: "name", Message: "boom"}},
		},
		{
			name:     "validation.Error with explicit fields is preserved",
			checkRet: validation.Error{Subject: "explicit", Field: "explicit-field", Message: "boom"},
			want:     validation.Errors{{Subject: "explicit", Field: "explicit-field", Message: "boom"}},
		},
		{
			name: "validation.Errors entries with empty Field are stamped",
			checkRet: validation.Errors{
				{Field: "explicit", Message: "boom1"},
				{Message: "boom2"},
			},
			want: validation.Errors{
				{Field: "explicit", Message: "boom1"},
				{Field: "name", Message: "boom2"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			check := func(string) error { return tt.checkRet }
			rule := validation.Field("name", "foo", check)
			require.Equal(t, tt.want, rule())
		})
	}
}

func TestField_RunsAllChecks(t *testing.T) {
	t.Parallel()

	failA := func(string) error { return errors.New("a") }
	failB := func(string) error { return errors.New("b") }
	rule := validation.Field("name", "foo", failA, failB)

	errs := rule()
	require.Len(t, errs, 2)
	require.Equal(t, "a", errs[0].Message)
	require.Equal(t, "b", errs[1].Message)
}

func TestField_DoesNotMutateCheckOutput(t *testing.T) {
	t.Parallel()

	// A check that returns the same validation.Errors value every time. If
	// Field mutates the returned slice in place, the second call will see
	// the entries' Field already populated and silently skip stamping.
	shared := validation.Errors{{Message: "boom"}}
	check := func(string) error { return shared }

	first := validation.Field("first", "x", check)()
	second := validation.Field("second", "x", check)()

	require.Len(t, first, 1)
	require.Len(t, second, 1)
	require.Equal(t, "first", first[0].Field)
	require.Equal(t, "second", second[0].Field, "second Field call must stamp independently")
	require.Empty(t, shared[0].Field, "the caller's Errors value must not be mutated")
}
