package validation_test

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/temporalio/temporal-proxy/pkg/validation"
)

func TestValidate_NoRules(t *testing.T) {
	t.Parallel()

	require.Nil(t, validation.Validate("subj"))
}

func TestValidate_AllRulesPass(t *testing.T) {
	t.Parallel()

	pass := func() validation.Errors { return nil }

	require.Nil(t, validation.Validate("subj", pass, pass))
}

func TestValidate_SubjectStamping(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		outerSubj   string
		ruleErr     validation.Error
		wantSubject string
	}{
		{
			name:        "empty Subject is stamped with outer",
			outerSubj:   "user-42",
			ruleErr:     validation.Error{Field: "name", Message: "is required"},
			wantSubject: "user-42",
		},
		{
			name:        "explicit Subject is preserved",
			outerSubj:   "outer",
			ruleErr:     validation.Error{Subject: "nested", Field: "name", Message: "boom"},
			wantSubject: "nested",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rule := func() validation.Errors { return validation.Errors{tt.ruleErr} }
			errs := validation.Validate(tt.outerSubj, rule)
			require.Len(t, errs, 1)
			require.Equal(t, tt.wantSubject, errs[0].Subject)
		})
	}
}

func TestValidate_ConcatenatesFromMultipleRules(t *testing.T) {
	t.Parallel()

	a := func() validation.Errors {
		return validation.Errors{{Field: "a", Message: "bad"}}
	}
	b := func() validation.Errors {
		return validation.Errors{
			{Field: "b", Message: "bad"},
			{Field: "c", Message: "bad"},
		}
	}

	errs := validation.Validate("subj", a, b)
	require.Len(t, errs, 3)

	// Errors implements error, so errors.As must still find the aggregate.
	var verrs validation.Errors
	require.True(t, errors.As(errs, &verrs))
	require.Len(t, verrs, 3)
}
