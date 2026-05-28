package validation

import (
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
)

var errInvalidHostPort = errors.New("is not a valid host:port")

// Check verifies a single value of type V. A nil return means valid.
type Check[V any] func(V) error

// IsHostPort validates that s is syntactically a host:port pair. The check is
// purely lexical: no DNS lookup, no /etc/services port resolution. Both
// "host:port" and ":port" (listen-on-all-interfaces) forms are valid, and the
// port must be a decimal integer in [0, 65535]. Port 0 is accepted because it
// is a valid listener form meaning "let the OS pick".
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
