package cloud_test

import (
	"errors"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/temporalio/temporal-proxy/internal/cloud"
	"github.com/temporalio/temporal-proxy/pkg/validation"
)

const (
	malformedID = "is malformed. Should be <name>.<account-id>"
	nameShape   = "must begin with a letter, end with a letter or number, and contain only letters, numbers, and the hyphen"
	nameSize    = "must be between 2 and 39 characters"
	accountSize = "must be between 5 and 20 characters"
)

func TestValidateAccountID(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		id       string
		wantErrs []validation.Error
	}{
		{name: "typical account id", id: "a1b2c"},
		{name: "longest", id: strings.Repeat("a", 20)},
		{
			name: "empty",
			id:   "",
			wantErrs: []validation.Error{
				{Field: "account-id", Message: "is required"},
				{Field: "account-id", Message: accountSize},
			},
		},
		{
			name:     "too short",
			id:       "a1b2",
			wantErrs: []validation.Error{{Field: "account-id", Message: accountSize}},
		},
		{
			name:     "too long",
			id:       strings.Repeat("a", 21),
			wantErrs: []validation.Error{{Field: "account-id", Message: accountSize}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := cloud.ValidateAccountID(tt.id)
			if len(tt.wantErrs) == 0 {
				require.NoError(t, err)
				return
			}

			var errs validation.Errors
			require.True(t, errors.As(err, &errs), "expected validation.Errors, got %T", err)
			require.ElementsMatch(t, tt.wantErrs, []validation.Error(errs))
		})
	}
}

func TestValidateNamespace(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		ns       string
		wantErrs []validation.Error
	}{
		{name: "typical namespace", ns: "my-namespace.a2dd6"},
		{name: "digits in name", ns: "ns1.a2dd6"},
		{name: "shortest name", ns: "ab.a2dd6"},
		{name: "longest name", ns: strings.Repeat("a", 39) + ".a2dd6"},
		{name: "longest account id", ns: "myns." + strings.Repeat("a", 20)},
		{name: "account id is not name-shaped", ns: "myns.A_2xy"},
		{
			name:     "empty",
			ns:       "",
			wantErrs: []validation.Error{{Field: "id", Message: malformedID}},
		},
		{
			name:     "no separator",
			ns:       "mynamespace",
			wantErrs: []validation.Error{{Field: "id", Message: malformedID}},
		},
		{
			name:     "extra separator",
			ns:       "my.name.space",
			wantErrs: []validation.Error{{Field: "id", Message: malformedID}},
		},
		{
			name: "missing name",
			ns:   ".a2dd6",
			wantErrs: []validation.Error{
				{Field: "name", Message: "is required"},
				{Field: "name", Message: nameSize},
				{Field: "name", Message: nameShape},
			},
		},
		{
			name: "missing account id",
			ns:   "myns.",
			wantErrs: []validation.Error{
				{Field: "account-id", Message: "is required"},
				{Field: "account-id", Message: accountSize},
			},
		},
		{
			name: "name too short",
			ns:   "a.a2dd6",
			wantErrs: []validation.Error{
				{Field: "name", Message: nameSize},
				{Field: "name", Message: nameShape},
			},
		},
		{
			name:     "name too long",
			ns:       strings.Repeat("a", 40) + ".a2dd6",
			wantErrs: []validation.Error{{Field: "name", Message: nameSize}},
		},
		{
			name: "uppercase name",
			ns:   "My-NS.a2dd6",
			wantErrs: []validation.Error{
				{Field: "name", Message: "must be lowercase"},
				{Field: "name", Message: nameShape},
			},
		},
		{
			name:     "name starts with a hyphen",
			ns:       "-myns.a2dd6",
			wantErrs: []validation.Error{{Field: "name", Message: nameShape}},
		},
		{
			name:     "name ends with a hyphen",
			ns:       "myns-.a2dd6",
			wantErrs: []validation.Error{{Field: "name", Message: nameShape}},
		},
		{
			name:     "name starts with a digit",
			ns:       "1myns.a2dd6",
			wantErrs: []validation.Error{{Field: "name", Message: nameShape}},
		},
		{
			name:     "underscore in name",
			ns:       "my_ns.a2dd6",
			wantErrs: []validation.Error{{Field: "name", Message: nameShape}},
		},
		{
			name:     "account id too short",
			ns:       "myns.a2dd",
			wantErrs: []validation.Error{{Field: "account-id", Message: accountSize}},
		},
		{
			name:     "account id too long",
			ns:       "myns." + strings.Repeat("a", 21),
			wantErrs: []validation.Error{{Field: "account-id", Message: accountSize}},
		},
		{
			name: "both labels bad",
			ns:   "-.abc",
			wantErrs: []validation.Error{
				{Field: "name", Message: nameSize},
				{Field: "name", Message: nameShape},
				{Field: "account-id", Message: accountSize},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := cloud.ValidateNamespace(tt.ns)
			if len(tt.wantErrs) == 0 {
				require.NoError(t, err)
				return
			}

			var errs validation.Errors
			require.True(t, errors.As(err, &errs), "expected validation.Errors, got %T", err)
			require.ElementsMatch(t, tt.wantErrs, []validation.Error(errs))
		})
	}
}
