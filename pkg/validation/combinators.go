package validation

import "fmt"

type (
	// Validator is satisfied by any type whose Validate method returns an
	// error. Types that already follow the standard "Validate() error"
	// convention satisfy this interface implicitly.
	Validator interface {
		Validate() error
	}

	validatorFunc func() error
)

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

// Nested embeds the result of v.Validate as a Rule, prefixing subject onto
// each entry to build a dotted path. An entry the child left unattributed gets
// Subject set to subject; an entry the child already attributed gets its
// Subject prefixed (subject + "." + child), so deeper nesting composes into a
// path like "classes[0].subjects[1]". A nil error from v yields no entries, an
// empty subject leaves entries unchanged, and any non-validation error is
// wrapped as a single entry whose Message is err.Error() and whose Subject is
// subject.
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
			} else {
				errs[i].Subject = subject + "." + errs[i].Subject
			}
		}

		return errs
	}
}

// Children is the slice counterpart to Nested: it validates each element of
// items with validate, prefixing a "name[i]" segment onto the resulting
// subjects to build a dotted path (e.g. "classes[0].subjects[1]"). Empty
// subjects become the segment; already-set ones are prefixed into a path. The
// per-element validate is supplied explicitly rather than via the Validator
// interface, so callers can thread external context by closing over it - for
// example Children("classes", c.Classes, func(x *Class) error { return x.validate(ctx) }).
// A nil or empty slice yields no entries and never calls validate.
func Children[S ~[]T, T any](name string, items S, validate func(*T) error) Rule {
	return func() Errors {
		var errs Errors
		for i := range items {
			item := &items[i]
			segment := fmt.Sprintf("%s[%d]", name, i)
			errs = append(errs, Nested(segment, validatorFunc(func() error {
				return validate(item)
			}))()...)
		}

		return errs
	}
}

func (f validatorFunc) Validate() error { return f() }
