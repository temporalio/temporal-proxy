package validation

// Rule produces zero or more validation errors when run.
type Rule func() Errors

// Field builds a Rule that runs every Check[V] against value and turns any
// failure into validation.Errors entries. The returned Errors have their
// Field stamped with name, unless the check returned a validation.Error or
// validation.Errors with Field already set, in which case the existing value
// is preserved.
func Field[V any](name string, value V, checks ...Check[V]) Rule {
	return func() Errors {
		var errs Errors
		for _, c := range checks {
			err := c(value)
			if err == nil {
				continue
			}
			errs = append(errs, toErrors(name, err)...)
		}
		return errs
	}
}
