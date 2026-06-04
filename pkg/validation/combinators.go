package validation

// Validator is satisfied by any type whose Validate method returns an
// error. Types that already follow the standard "Validate() error"
// convention satisfy this interface implicitly.
type Validator interface {
	Validate() error
}

// When runs the supplied checks only when pred(v) returns true. When pred
// returns false the combinator yields nil and no inner check runs. Inner
// failures are aggregated into a single Errors return.
func When[V any](pred func(V) bool, checks ...Check[V]) Check[V] {
	return func(v V) error {
		if !pred(v) {
			return nil
		}
		var errs Errors
		for _, c := range checks {
			if err := c(v); err != nil {
				errs = append(errs, toErrors("", err)...)
			}
		}
		if len(errs) == 0 {
			return nil
		}
		return errs
	}
}

// WhenFn is When with a value-free predicate. Use it to guard checks on
// captured outer state - for example, "if cfg.TLS != nil, require this
// field" - where the predicate does not depend on the value under
// validation.
func WhenFn[V any](pred func() bool, checks ...Check[V]) Check[V] {
	return When(func(V) bool { return pred() }, checks...)
}

// WhenRules runs the supplied rules only when pred returns true. When
// pred returns false the combinator yields no entries. Use this to skip
// an entire block of Field rules at once (for example, every TLS.* field
// when TLS is not configured).
func WhenRules(pred func() bool, rules ...Rule) Rule {
	return func() Errors {
		if !pred() {
			return nil
		}
		var errs Errors
		for _, r := range rules {
			errs = append(errs, r()...)
		}
		return errs
	}
}

// Nested embeds the result of v.Validate as a Rule. Entries whose
// Subject was left empty by the child are stamped with subject; entries
// the child already attributed (or whose Subject is non-empty for any
// reason) are preserved unchanged. A nil error from v yields no entries,
// and any non-validation error is wrapped as a single entry whose
// Message is err.Error() and whose Subject is subject.
func Nested(subject string, v Validator) Rule {
	return func() Errors {
		err := v.Validate()
		if err == nil {
			return nil
		}

		errs := toErrors("", err)
		if subject == "" {
			return errs
		}

		for i := range errs {
			if errs[i].Subject == "" {
				errs[i].Subject = subject
			}
		}

		return errs
	}
}
