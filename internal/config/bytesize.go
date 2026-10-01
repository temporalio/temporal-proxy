package config

import (
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
)

var (
	// byteUnitOrder lists the units largest first, for formatting.
	byteUnitOrder = []string{"GiB", "GB", "MiB", "MB", "KiB", "KB"}

	// byteUnits are the accepted size suffixes. As in Kubernetes quantities, the
	// SI units are decimal (MB is 10^6) and the IEC units are binary (MiB is 2^20).
	byteUnits = map[string]int64{
		"":    1,
		"B":   1,
		"KB":  1_000,
		"MB":  1_000_000,
		"GB":  1_000_000_000,
		"KiB": 1 << 10,
		"MiB": 1 << 20,
		"GiB": 1 << 30,
	}

	errInvalidByteSize = errors.New("invalid byte size")
)

// ByteSize is a size in bytes, written in YAML as a bare integer or an integer
// with a unit suffix, such as "128MiB". KB, MB, and GB are decimal; KiB, MiB,
// and GiB are binary.
type ByteSize int64

// ParseByteSize parses a whole, non-negative number of bytes with an optional
// unit suffix: B, the decimal KB, MB, and GB, or the binary KiB, MiB, and GiB.
func ParseByteSize(s string) (ByteSize, error) {
	s = strings.TrimSpace(s)
	digits := strings.TrimLeft(s, "0123456789")
	num, unit := s[:len(s)-len(digits)], strings.TrimSpace(digits)

	if num == "" {
		return 0, fmt.Errorf("%w %q: expected a whole number with an optional unit, like 128MiB", errInvalidByteSize, s)
	}

	mult, ok := byteUnits[unit]
	if !ok {
		if strings.ContainsAny(unit, ".-") {
			return 0, fmt.Errorf("%w %q: expected a whole number with an optional unit, like 128MiB", errInvalidByteSize, s)
		}

		return 0, fmt.Errorf("%w %q: unknown unit %q, expected one of B, KB, MB, GB, KiB, MiB, GiB", errInvalidByteSize, s, unit)
	}

	n, err := strconv.ParseInt(num, 10, 64)
	if err != nil || n > math.MaxInt64/mult {
		return 0, fmt.Errorf("%w %q: overflows a 64-bit size", errInvalidByteSize, s)
	}

	return ByteSize(n * mult), nil
}

// UnmarshalYAML decodes a bare integer or a string with a unit suffix. A bare
// integer arrives as a YAML number, so it is decoded as text and parsed the same
// way as a suffixed size.
func (b *ByteSize) UnmarshalYAML(unmarshal func(any) error) error {
	var decoded string
	if err := unmarshal(&decoded); err != nil {
		return err
	}

	size, err := ParseByteSize(decoded)
	if err != nil {
		return err
	}

	*b = size
	return nil
}

// String formats the size in the largest unit that divides it evenly, binary
// before decimal at the same scale, so it reads back the way it was written.
func (b ByteSize) String() string {
	for _, unit := range byteUnitOrder {
		if mult := ByteSize(byteUnits[unit]); b != 0 && b%mult == 0 {
			return fmt.Sprintf("%d%s", b/mult, unit)
		}
	}

	return fmt.Sprintf("%dB", int64(b))
}
