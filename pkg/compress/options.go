package compress

import (
	"errors"
	"fmt"
)

const (
	// defaultMaxDecodedSize matches gRPC's default receive limit, which bounds
	// the largest request (and so the largest payload) the gateway accepts.
	defaultMaxDecodedSize = 4 << 20 // 4MiB

	// minMaxDecodedSize is the largest window a Zstd frame declares for a small
	// payload. The Zstd decoder also caps the window at the max decoded size, so
	// anything lower would reject valid payloads.
	minMaxDecodedSize = 2 << 10 // 2KiB
)

// ErrTooLarge is returned by Decompress when the decoded output would exceed
// the compressor's maximum decoded size (see [WithMaxDecodedSize]).
var ErrTooLarge = errors.New("decoded size exceeds limit")

type (
	// Option configures a compressor.
	Option func(*options)

	options struct {
		maxDecodedSize int
	}
)

// WithMaxDecodedSize caps how many bytes a single Decompress call may produce;
// larger output fails with [ErrTooLarge]. n must be at least 2KiB. Defaults to
// 4MiB.
func WithMaxDecodedSize(n int) Option {
	return func(o *options) {
		o.maxDecodedSize = n
	}
}

func buildOptions(opts []Option) (options, error) {
	o := options{maxDecodedSize: defaultMaxDecodedSize}
	for _, opt := range opts {
		opt(&o)
	}

	if o.maxDecodedSize < minMaxDecodedSize {
		return options{}, fmt.Errorf(
			"invalid max decoded size %d: must be at least %d",
			o.maxDecodedSize,
			minMaxDecodedSize,
		)
	}

	return o, nil
}
