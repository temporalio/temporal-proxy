package proxy

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.temporal.io/proxy/internal/config"
	"go.temporal.io/proxy/pkg/codec"
	"go.temporal.io/proxy/pkg/compress"
)

// decodeLevel is the level of the zstd compressor in the decoder set. Decoding
// does not depend on the level a payload was compressed at, so any valid level
// restores every zstd payload.
const decodeLevel = 3

type (
	// compression is what a [Codecs] compresses and restores payloads with, all
	// built once, so a request only binds an observer to it.
	compression struct {
		settings *config.Compression
		reporter *CompressionReporter
		decoders codec.CompressorOption
		encoders map[config.CompressionSettings]encoder

		// none is the decode-only option outbound uses for a namespace that doesn't
		// compress. It has nothing to observe there, so it is shared as is.
		none codec.Option
	}

	// encoder is one enabled settings value's compressor and threshold.
	encoder struct {
		cmp      codec.Compression
		minBytes codec.CompressorOption
	}

	// compressions builds the compressors a [Codecs] uses, at most one per
	// algorithm and zstd level, since a zstd encoder is expensive to create and
	// safe to share.
	compressions struct {
		built map[compressionKey]codec.Compression
	}

	// compressionKey identifies one compressor. Level is only set for zstd, the
	// one algorithm that has levels.
	compressionKey struct {
		algorithm config.CompressionAlgorithm
		level     int
	}

	// observer binds a [CompressionReporter] to a single request: the context its
	// metadata labels are read from and the local namespace it is labeled by. It
	// is the [codec.CompressionObserver] the compression codec reports through.
	observer struct {
		ctx context.Context
		ns  string
		r   *CompressionReporter
	}
)

// newCompression returns the compression c and r imply: a decoder set that
// restores payloads compressed with any algorithm, and an encoder for each
// enabled settings value in c. A nil c compresses nothing, and a nil r records
// nothing.
func newCompression(c *config.Compression, r *CompressionReporter) (*compression, error) {
	cs := &compressions{built: make(map[compressionKey]codec.Compression)}

	decoders := make([]codec.Compression, 0, 3)
	for _, k := range []compressionKey{
		{algorithm: config.CompressionLZ4},
		{algorithm: config.CompressionSnappy},
		{algorithm: config.CompressionZstd, level: decodeLevel},
	} {
		cmp, err := cs.get(k)
		if err != nil {
			return nil, err
		}

		decoders = append(decoders, cmp)
	}

	// Built once and shared by every Compressor, so none rebuilds the set.
	res := &compression{
		settings: c,
		reporter: r,
		decoders: codec.WithDecoders(decoders...),
		encoders: make(map[config.CompressionSettings]encoder),
	}
	res.none = codec.WithCompression(nil, res.decoders)

	if c == nil {
		return res, nil
	}

	for s := range c.All() {
		if _, ok := res.encoders[s]; ok || !s.Enabled {
			continue
		}

		k := compressionKey{algorithm: s.Algorithm}
		if s.Algorithm == config.CompressionZstd {
			k.level = s.Level
		}

		enc, err := cs.get(k)
		if err != nil {
			return nil, err
		}

		res.encoders[s] = encoder{cmp: enc, minBytes: codec.WithMinBytes(s.MinBytes)}
	}

	return res, nil
}

// decode returns the option that restores a request's payloads.
func (c *compression) decode(ctx context.Context, s scope) codec.Option {
	return codec.WithCompression(nil, c.decoders, c.observe(ctx, s))
}

// encode returns the option that compresses a request's payloads with the
// settings of its local namespace.
func (c *compression) encode(ctx context.Context, s scope) codec.Option {
	enc, ok := c.encoders[c.settings.Settings(s.ns)]
	if !ok {
		return c.none
	}

	return codec.WithCompression(enc.cmp, c.decoders, enc.minBytes, c.observe(ctx, s))
}

// observe returns the option that reports a request's payloads, which reports
// nothing without a reporter.
func (c *compression) observe(ctx context.Context, s scope) codec.CompressorOption {
	// Checked here so a nil reporter never reaches the codec inside a non-nil
	// interface.
	if c.reporter == nil {
		return codec.WithCompressionObserver(nil)
	}

	return codec.WithCompressionObserver(&observer{ctx: ctx, ns: s.ns, r: c.reporter})
}

// get returns the compressor for k, building it the first time it is asked for.
func (cs *compressions) get(k compressionKey) (codec.Compression, error) {
	if cmp, ok := cs.built[k]; ok {
		return cmp, nil
	}

	var (
		cmp codec.Compression
		err error
	)

	switch k.algorithm {
	case config.CompressionLZ4:
		cmp, err = compress.NewLZ4()
	case config.CompressionSnappy:
		cmp, err = compress.NewSnappy()
	case config.CompressionZstd:
		cmp, err = compress.NewZstd(k.level)
	default:
		err = fmt.Errorf("unknown algorithm %q", string(k.algorithm))
	}

	if err != nil {
		return nil, fmt.Errorf("proxy: failed to build %s compressor: %w", k.algorithm, err)
	}

	cs.built[k] = cmp

	return cmp, nil
}

// Observe records one payload compressed or restored, and on a successful
// compress, its size either side.
func (o *observer) Observe(op, algorithm string, in, out int, d time.Duration, err error) {
	algorithm = algorithmLabel(algorithm)
	o.r.Op(o.ctx, op, algorithm, compressionResult(err), o.ns, d.Seconds())

	if op == codec.CompressionOpCompress && err == nil {
		o.r.PayloadBytes(o.ctx, "pre_compress", algorithm, o.ns, in)
		o.r.PayloadBytes(o.ctx, "post_compress", algorithm, o.ns, out)
	}
}

// Skipped records one payload forwarded uncompressed.
func (o *observer) Skipped(reason, algorithm string) {
	o.r.PayloadSkipped(o.ctx, reason, algorithmLabel(algorithm), o.ns)
}

// algorithmLabel maps an algorithm name to the "algorithm" metric label value.
// On decode the name comes from payload metadata any writer to the namespace
// controls, so anything this build doesn't know is reported as unknown rather
// than becoming a series of its own.
func algorithmLabel(name string) string {
	switch a := config.CompressionAlgorithm(name); a {
	case config.CompressionLZ4, config.CompressionSnappy, config.CompressionZstd:
		return string(a)
	default:
		return "unknown"
	}
}

// compressionResult maps an error to the "result" metric label value. Output
// past the decode cap and an algorithm this build has no decoder for get their
// own values, since each calls for a different fix than corrupt data does.
func compressionResult(err error) string {
	switch {
	case err == nil:
		return "success"
	case errors.Is(err, compress.ErrTooLarge):
		return "too_large"
	case errors.Is(err, codec.ErrUnknownAlgorithm):
		return "unknown_algorithm"
	default:
		return "error"
	}
}
