package validation

import (
	"cmp"
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
)

var (
	errInvalidHostPort = errors.New("is not a valid host:port")
	errInvalidHostname = errors.New("is not a valid hostname")
)

// Check verifies a single value of type V. A nil return means valid.
type Check[V any] func(V) error

// IsHostname validates that s is a DNS name: non-empty labels of ASCII
// letters, digits, '-' and '_' (for SRV labels) joined by dots, with a single
// trailing dot allowed for a fully-qualified name. Like IsHostPort, the check
// is purely lexical.
func IsHostname() Check[string] {
	return func(s string) error {
		s = strings.TrimSuffix(s, ".")
		if s == "" {
			return errInvalidHostname
		}

		for label := range strings.SplitSeq(s, ".") {
			if label == "" || strings.ContainsFunc(label, func(r rune) bool { return !isLabelRune(r) }) {
				return errInvalidHostname
			}
		}

		return nil
	}
}

// IsHostPort validates that s is syntactically a host:port pair. The check is
// purely lexical: no DNS lookup, no /etc/services port resolution. Both
// "host:port" and ":port" (listen-on-all-interfaces) forms are valid, and the
// port must be a decimal integer in [0, 65535]. Port 0 is accepted because it
// is a valid listener form meaning "let the OS pick". A host containing "/" is
// rejected.
func IsHostPort() Check[string] {
	return func(s string) error {
		host, port, err := net.SplitHostPort(s)
		if err != nil {
			return errInvalidHostPort
		}

		if strings.ContainsAny(host, "/") {
			return errInvalidHostPort
		}

		p, err := strconv.Atoi(port)
		if err != nil || p < 0 || p > 65535 {
			return errInvalidHostPort
		}

		return nil
	}
}

// GT rejects any value not strictly greater than mark; a value equal to mark
// fails. It works for any cmp.Ordered type (numbers, strings), comparing with
// the language's comparison operators, so for floats a NaN bound or input
// always passes the check.
func GT[V cmp.Ordered](mark V) Check[V] {
	return func(v V) error {
		if v <= mark {
			return fmt.Errorf("not greater than %v", mark)
		}

		return nil
	}
}

// GTE rejects any value less than mark; a value equal to mark passes. It is the
// inclusive counterpart to GT and shares its cmp.Ordered semantics; a NaN bound
// or input always passes.
func GTE[V cmp.Ordered](mark V) Check[V] {
	return func(v V) error {
		if v < mark {
			return fmt.Errorf("not greater than or equal to %v", mark)
		}

		return nil
	}
}

// LT rejects any value not strictly less than mark; a value equal to mark
// fails. It is the mirror of GT and shares its cmp.Ordered semantics; a NaN
// bound or input always passes.
func LT[V cmp.Ordered](mark V) Check[V] {
	return func(v V) error {
		if v >= mark {
			return fmt.Errorf("not less than %v", mark)
		}

		return nil
	}
}

// Required rejects the zero value of V. Note that this means Required[bool]()
// rejects false; reach for a different check when false is a meaningful value.
func Required[V comparable]() Check[V] {
	return func(v V) error {
		var zero V
		if v == zero {
			return errors.New("is required")
		}

		return nil
	}
}

// Unique rejects a slice containing two or more equal elements. The error
// message names the first duplicate encountered.
func Unique[V comparable]() Check[[]V] {
	return func(vs []V) error {
		seen := make(map[V]struct{}, len(vs))
		for _, v := range vs {
			if _, dup := seen[v]; dup {
				return fmt.Errorf("contains duplicate value: %v", v)
			}

			seen[v] = struct{}{}
		}

		return nil
	}
}

// isLabelRune reports whether r may appear in a hostname label.
func isLabelRune(r rune) bool {
	return r == '-' || r == '_' || r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9'
}
