package validation

import (
	"errors"
	"fmt"
)

type (
	// Error is a single validation failure for a named subject and field.
	Error struct {
		Subject string // identifier of the thing being validated (e.g. a hostname or cert CN)
		Field   string // attribute or property that failed (e.g. "expiry", "key_type")
		Message string // human-readable description of the failure
	}

	// Errors is a collection of Error values.
	Errors []Error
)

// Error implements the error interface.
func (e Error) Error() string {
	return fmt.Sprintf("%s: %s: %s", e.Subject, e.Field, e.Message)
}

// Error implements the error interface, joining all individual errors.
func (ve Errors) Error() string {
	if len(ve) == 0 {
		return ""
	}

	errs := make([]error, len(ve))
	for i, e := range ve {
		errs[i] = e
	}

	return errors.Join(errs...).Error()
}

// Unwrap returns each Error as an individual error, enabling errors.As and errors.Is traversal.
func (ve Errors) Unwrap() []error {
	errs := make([]error, len(ve))
	for i, e := range ve {
		errs[i] = e
	}

	return errs
}
