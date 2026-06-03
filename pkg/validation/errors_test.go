package validation_test

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/temporalio/temporal-proxy/pkg/validation"
)

func TestErrorError(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		subject  string
		field    string
		message  string
		expected string
	}{
		{
			name:     "basic format",
			subject:  "example.com",
			field:    "expiry",
			message:  "certificate has expired",
			expected: `example.com: expiry: certificate has expired`,
		},
		{
			name:     "with special characters",
			subject:  "*.temporal.io",
			field:    "key_type",
			message:  "expected RSA but got ECDSA",
			expected: `*.temporal.io: key_type: expected RSA but got ECDSA`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ve := validation.Error{
				Subject: tt.subject,
				Field:   tt.field,
				Message: tt.message,
			}
			require.Equal(t, tt.expected, ve.Error())
		})
	}
}

func TestErrorsError(t *testing.T) {
	t.Parallel()

	t.Run("empty slice returns empty string", func(t *testing.T) {
		t.Parallel()
		ve := validation.Errors{}
		require.Equal(t, "", ve.Error())
	})

	t.Run("single error", func(t *testing.T) {
		t.Parallel()
		ve := validation.Errors{
			{
				Subject: "cert1.com",
				Field:   "expiry",
				Message: "expired",
			},
		}
		errStr := ve.Error()
		require.Contains(t, errStr, `cert1.com: expiry: expired`)
	})

	t.Run("multiple errors", func(t *testing.T) {
		t.Parallel()
		ve := validation.Errors{
			{
				Subject: "cert1.com",
				Field:   "expiry",
				Message: "expired",
			},
			{
				Subject: "cert2.com",
				Field:   "is_ca",
				Message: "not a CA certificate",
			},
			{
				Subject: "cert3.com",
				Field:   "signature_algorithm",
				Message: "unsupported algorithm",
			},
		}
		errStr := ve.Error()
		require.Contains(t, errStr, `cert1.com: expiry: expired`)
		require.Contains(t, errStr, `cert2.com: is_ca: not a CA certificate`)
		require.Contains(t, errStr, `cert3.com: signature_algorithm: unsupported algorithm`)
	})
}

func TestErrorsUnwrap(t *testing.T) {
	t.Parallel()

	t.Run("empty slice", func(t *testing.T) {
		t.Parallel()
		ve := validation.Errors{}
		unwrapped := ve.Unwrap()
		require.Len(t, unwrapped, 0)
	})

	t.Run("single error", func(t *testing.T) {
		t.Parallel()
		ve := validation.Errors{
			{
				Subject: "cert1.com",
				Field:   "expiry",
				Message: "expired",
			},
		}
		unwrapped := ve.Unwrap()
		require.Len(t, unwrapped, 1)
	})

	t.Run("multiple errors", func(t *testing.T) {
		t.Parallel()
		ve := validation.Errors{
			{
				Subject: "cert1.com",
				Field:   "expiry",
				Message: "expired",
			},
			{
				Subject: "cert2.com",
				Field:   "is_ca",
				Message: "not a CA certificate",
			},
		}
		unwrapped := ve.Unwrap()
		require.Len(t, unwrapped, 2)
	})
}

func TestErrorsAsError(t *testing.T) {
	t.Parallel()

	ve := validation.Errors{
		{
			Subject: "cert1.com",
			Field:   "expiry",
			Message: "expired",
		},
		{
			Subject: "cert2.com",
			Field:   "is_ca",
			Message: "not a CA certificate",
		},
	}

	var validationErr validation.Error
	require.True(t, errors.As(ve, &validationErr))
	require.Equal(t, "cert1.com", validationErr.Subject)
	require.Equal(t, "expiry", validationErr.Field)
	require.Equal(t, "expired", validationErr.Message)
}
