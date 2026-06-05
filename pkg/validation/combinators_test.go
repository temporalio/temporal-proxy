package validation_test

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/temporalio/temporal-proxy/pkg/validation"
)

func TestWhen_PredicateFalse_DoesNotInvokeChecks(t *testing.T) {
	t.Parallel()

	var calls int
	failing := func(string) error {
		calls++
		return errors.New("should not run")
	}

	c := validation.When(func(string) bool { return false }, failing)
	require.NoError(t, c("anything"))
	require.Zero(t, calls)
}

func TestWhen_PredicateTrue_AllPass_ReturnsNil(t *testing.T) {
	t.Parallel()

	ok := func(string) error { return nil }
	c := validation.When(func(string) bool { return true }, ok, ok)
	require.NoError(t, c("anything"))
}

func TestWhen_PredicateTrue_AggregatesFailures(t *testing.T) {
	t.Parallel()

	failA := func(string) error { return errors.New("a") }
	failB := func(string) error { return errors.New("b") }

	c := validation.When(func(string) bool { return true }, failA, failB)
	err := c("anything")
	require.Error(t, err)

	var errs validation.Errors
	require.True(t, errors.As(err, &errs), "expected validation.Errors, got %T", err)
	require.Len(t, errs, 2)
	require.Equal(t, "a", errs[0].Message)
	require.Equal(t, "b", errs[1].Message)
}

func TestWhen_PredicateReceivesValue(t *testing.T) {
	t.Parallel()

	var seen string
	pred := func(s string) bool {
		seen = s
		return false
	}

	c := validation.When(pred, func(string) error { return nil })
	require.NoError(t, c("hello"))
	require.Equal(t, "hello", seen)
}

func TestWhen_PreservesExplicitField(t *testing.T) {
	t.Parallel()

	fail := func(string) error {
		return validation.Error{Field: "explicit", Message: "boom"}
	}
	c := validation.When(func(string) bool { return true }, fail)

	err := c("anything")
	var errs validation.Errors
	require.True(t, errors.As(err, &errs))
	require.Len(t, errs, 1)
	require.Equal(t, "explicit", errs[0].Field)
	require.Equal(t, "boom", errs[0].Message)
}

func TestWhen_EmptyChecksList_PredicateTrue_ReturnsNil(t *testing.T) {
	t.Parallel()

	c := validation.When[string](func(string) bool { return true })
	require.NoError(t, c("anything"))
}

func TestWhenFn_PredicateFalse_DoesNotInvokeChecks(t *testing.T) {
	t.Parallel()

	var calls int
	failing := func(string) error {
		calls++
		return errors.New("should not run")
	}

	c := validation.WhenFn[string](func() bool { return false }, failing)
	require.NoError(t, c("anything"))
	require.Zero(t, calls)
}

func TestWhenFn_PredicateTrue_RunsChecks(t *testing.T) {
	t.Parallel()

	fail := func(string) error { return errors.New("boom") }
	c := validation.WhenFn[string](func() bool { return true }, fail)

	err := c("anything")
	require.Error(t, err)

	var errs validation.Errors
	require.True(t, errors.As(err, &errs))
	require.Len(t, errs, 1)
	require.Equal(t, "boom", errs[0].Message)
}

func TestWhenFn_CapturedState(t *testing.T) {
	t.Parallel()

	// Demonstrates the intended use: the predicate closes over outer state.
	tlsEnabled := false
	c := validation.WhenFn[string](
		func() bool { return tlsEnabled },
		func(string) error { return errors.New("required") },
	)

	require.NoError(t, c(""))
	tlsEnabled = true
	require.Error(t, c(""))
}

func TestWhenRules_PredicateFalse_DoesNotInvokeRules(t *testing.T) {
	t.Parallel()

	var calls int
	rule := validation.Rule(func() validation.Errors {
		calls++
		return validation.Errors{{Message: "should not run"}}
	})

	out := validation.WhenRules(func() bool { return false }, rule)()
	require.Empty(t, out)
	require.Zero(t, calls)
}

func TestWhenRules_PredicateTrue_RunsAllRules(t *testing.T) {
	t.Parallel()

	ruleA := validation.Rule(func() validation.Errors {
		return validation.Errors{{Field: "a", Message: "boom-a"}}
	})
	ruleB := validation.Rule(func() validation.Errors {
		return validation.Errors{{Field: "b", Message: "boom-b"}}
	})

	out := validation.WhenRules(func() bool { return true }, ruleA, ruleB)()
	require.Len(t, out, 2)
	require.Equal(t, "a", out[0].Field)
	require.Equal(t, "b", out[1].Field)
}

func TestWhenRules_EmptyRulesList_IsNoOp(t *testing.T) {
	t.Parallel()

	out := validation.WhenRules(func() bool { return true })()
	require.Empty(t, out)
}

func TestWhenRules_IntegratesWithValidate(t *testing.T) {
	t.Parallel()

	// WhenRules wrapped in Validate should let Validate's subject stamping
	// apply to the inner Field's emitted entries.
	rule := validation.Field("inner", "", validation.Required[string]())

	err := validation.Validate(
		"subject",
		validation.WhenRules(func() bool { return true }, rule),
	)

	var errs validation.Errors
	require.True(t, errors.As(err, &errs))
	require.Len(t, errs, 1)
	require.Equal(t, "subject", errs[0].Subject)
	require.Equal(t, "inner", errs[0].Field)
}

type recordingValidator struct {
	calls int
	err   error
}

func (r *recordingValidator) Validate() error {
	r.calls++
	return r.err
}

func TestNested_NilError_NoEntries(t *testing.T) {
	t.Parallel()

	v := &recordingValidator{err: nil}
	out := validation.Nested("tls", v)()
	require.Empty(t, out)
	require.Equal(t, 1, v.calls)
}

func TestNested_StampsEmptySubjects(t *testing.T) {
	t.Parallel()

	v := &recordingValidator{
		err: validation.Errors{
			{Field: "certFile", Message: "is required"},
			{Field: "keyFile", Message: "is required"},
		},
	}

	out := validation.Nested("tls", v)()
	require.Len(t, out, 2)
	require.Equal(t, "tls", out[0].Subject)
	require.Equal(t, "tls", out[1].Subject)
	require.Equal(t, "certFile", out[0].Field)
	require.Equal(t, "keyFile", out[1].Field)
}

func TestNested_PreservesExplicitSubjects(t *testing.T) {
	t.Parallel()

	v := &recordingValidator{
		err: validation.Errors{
			{Subject: "explicit", Field: "x", Message: "boom"},
			{Field: "y", Message: "boom"},
		},
	}

	out := validation.Nested("outer", v)()
	require.Len(t, out, 2)
	require.Equal(t, "explicit", out[0].Subject, "explicit subject must be preserved")
	require.Equal(t, "outer", out[1].Subject, "empty subject must be stamped")
}

func TestNested_SingleValidationError(t *testing.T) {
	t.Parallel()

	v := &recordingValidator{
		err: validation.Error{Field: "certFile", Message: "is required"},
	}

	out := validation.Nested("tls", v)()
	require.Len(t, out, 1)
	require.Equal(t, "tls", out[0].Subject)
	require.Equal(t, "certFile", out[0].Field)
}

func TestNested_PlainError_WrappedAsSingleEntry(t *testing.T) {
	t.Parallel()

	v := &recordingValidator{err: errors.New("io: file not found")}

	out := validation.Nested("tls", v)()
	require.Len(t, out, 1)
	require.Equal(t, "tls", out[0].Subject)
	require.Equal(t, "", out[0].Field)
	require.Equal(t, "io: file not found", out[0].Message)
}

func TestNested_EmptySubject_NoStamping(t *testing.T) {
	t.Parallel()

	v := &recordingValidator{
		err: validation.Errors{{Field: "x", Message: "boom"}},
	}

	out := validation.Nested("", v)()
	require.Len(t, out, 1)
	require.Equal(t, "", out[0].Subject, "empty subject argument must not stamp")
	require.Equal(t, "x", out[0].Field)
}

func TestNested_NotInvokedUnderFalseWhenRules(t *testing.T) {
	t.Parallel()

	v := &recordingValidator{err: errors.New("should not run")}

	rule := validation.WhenRules(
		func() bool { return false },
		validation.Nested("tls", v),
	)

	out := rule()
	require.Empty(t, out)
	require.Zero(t, v.calls, "Validator must not be invoked when guard is false")
}

func TestNested_OuterValidate_DoesNotReStampExplicitSubject(t *testing.T) {
	t.Parallel()

	v := &recordingValidator{
		err: validation.Errors{{Field: "x", Message: "boom"}},
	}

	// Nested stamps "tls" first; Validate's outer "outer" subject must not
	// re-stamp because the entry's Subject is no longer empty.
	err := validation.Validate("outer", validation.Nested("tls", v))
	var errs validation.Errors
	require.True(t, errors.As(err, &errs))
	require.Len(t, errs, 1)
	require.Equal(t, "tls", errs[0].Subject)
}
