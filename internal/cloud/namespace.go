package cloud

import (
	"errors"
	"fmt"
	"regexp"
	"strings"

	"github.com/temporalio/temporal-proxy/pkg/validation"
)

const (
	nameMinLen = 2
	nameMaxLen = 39

	accountIDMinLen = 5
	accountIDMaxLen = 20
)

// start with letter, end with letter/number, contain only a-z0-9-
var nsNameRegex = regexp.MustCompile(`^[a-z][a-z0-9-]*[a-z0-9]$`)

// ValidateAccountID checks that id is shaped like the account-id label of a
// Temporal Cloud namespace.
//
// See: https://docs.temporal.io/cloud/namespaces for details
func ValidateAccountID(id string) error {
	return validation.Validate("", accountIDRule(id))
}

// ValidateNamespace checks that ns is a well-formed Temporal Cloud namespace
// identifier, meaning "<name>.<account-id>". Every broken rule is reported, not
// just the first, so a caller can show the whole story at once.
//
// See: https://docs.temporal.io/cloud/namespaces for details
func ValidateNamespace(ns string) error {
	// Exactly one separator: without it there is no account id, and with more
	// than one there is no telling which label is which.
	name, accountID, found := strings.Cut(ns, ".")
	malformed := !found || strings.Contains(accountID, ".")

	return validation.Validate(
		"",
		validation.WhenRules(
			func() bool { return malformed },
			func() validation.Errors {
				return validation.Errors{{
					Field:   "id",
					Message: "is malformed. Should be <name>.<account-id>",
				}}
			},
		),
		validation.WhenRules(
			func() bool { return !malformed },
			validation.Field(
				"name",
				name,
				validation.Required[string](),
				size(nameMinLen, nameMaxLen),
				func(v string) error {
					if v != strings.ToLower(v) {
						return errors.New("must be lowercase")
					}

					return nil
				},
				func(v string) error {
					if !nsNameRegex.MatchString(v) {
						return errors.New("must begin with a letter, end with a letter or number, and contain only letters, numbers, and the hyphen")
					}

					return nil
				},
			),
			accountIDRule(accountID),
		),
	)
}

// accountIDRule builds the account-id checks, shared by [ValidateAccountID] and
// the second label of [ValidateNamespace] so the rule is defined once.
func accountIDRule(id string) validation.Rule {
	return validation.Field(
		"account-id",
		id,
		validation.Required[string](),
		size(accountIDMinLen, accountIDMaxLen),
	)
}

// size builds a check rejecting strings shorter than n or longer than m, both
// inclusive.
func size(n, m int) validation.Check[string] {
	return func(s string) error {
		if len(s) < n || len(s) > m {
			return fmt.Errorf("must be between %d and %d characters", n, m)
		}

		return nil
	}
}
