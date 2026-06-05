package validation

import (
	"errors"
	"fmt"
	"net"
)

// Check verifies a single value of type V. A nil return means valid.
type Check[V any] func(V) error

// IsHostPort validates that s is a host:port pair accepted by net.ResolveTCPAddr.
// Both "host:port" and ":port" (listen-on-all-interfaces) forms are valid.
func IsHostPort() Check[string] {
	return func(s string) error {
		if _, err := net.ResolveTCPAddr("tcp", s); err != nil {
			return errors.New("is not a valid host:port")
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
