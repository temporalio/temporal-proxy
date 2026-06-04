package validation

// Validate runs each rule, accumulating failures into a single error (which
// will typically be an [Errors] instance). Any [Error] whose Subject is empty is
// stamped with subject.
func Validate(subject string, rules ...Rule) error {
	var errs Errors
	for _, r := range rules {
		errs = append(errs, r()...)
	}

	if len(errs) == 0 {
		return nil
	}

	for i := range errs {
		if errs[i].Subject == "" {
			errs[i].Subject = subject
		}
	}

	return errs
}

func toErrors(field string, err error) Errors {
	if ves, ok := err.(Errors); ok {
		out := make(Errors, len(ves))
		copy(out, ves)
		for i := range out {
			if out[i].Field == "" {
				out[i].Field = field
			}
		}

		return out
	}

	if ve, ok := err.(Error); ok {
		if ve.Field == "" {
			ve.Field = field
		}

		return Errors{ve}
	}

	return Errors{{Field: field, Message: err.Error()}}
}
