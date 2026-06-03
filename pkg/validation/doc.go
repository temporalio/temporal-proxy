// Package validation provides structured error primitives plus a small
// rule-based API for accumulating validation failures.
//
// An [Error] captures a single failure as a (Subject, Field, Message) triple,
// where Subject identifies the thing being validated (e.g. a hostname or
// certificate CN), Field names the attribute that failed, and Message
// describes the failure in human-readable form. [Errors] aggregates multiple
// [Error] values into a single error while remaining compatible with
// [errors.Is], [errors.As], and [errors.Join] via its Unwrap method.
//
// Higher-level validation is expressed by passing a list of [Rule] values to
// [Validate], typically constructed via [Field] and the built-in [Check]
// functions ([Required], [Unique]). Validate accumulates every rule's output
// into one Errors value and stamps the supplied subject onto entries that
// don't already carry one.
package validation
