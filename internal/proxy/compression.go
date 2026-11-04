package proxy

import (
	"fmt"

	"go.temporal.io/proxy/internal/config"
	"go.temporal.io/proxy/pkg/codec"
	"go.temporal.io/proxy/pkg/compress"
)

// decodeLevel is the level of the zstd compressor in the decoder set. Decoding
// does not depend on the level a payload was compressed at, so any valid level
// restores every zstd payload.
const decodeLevel = 3

type (
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
)

// newCompression returns the decode-only option every inbound chain shares,
// which restores payloads compressed with any algorithm, and an option for each
// enabled settings value in c, keyed by it. A nil c compresses nothing.
func newCompression(c *config.Compression) (codec.Option, map[config.CompressionSettings]codec.Option, error) {
	cs := &compressions{built: make(map[compressionKey]codec.Compression)}

	decoders := make([]codec.Compression, 0, 3)
	for _, k := range []compressionKey{
		{algorithm: config.CompressionLZ4},
		{algorithm: config.CompressionSnappy},
		{algorithm: config.CompressionZstd, level: decodeLevel},
	} {
		cmp, err := cs.get(k)
		if err != nil {
			return nil, nil, err
		}

		decoders = append(decoders, cmp)
	}

	// Built once and shared by every option, so each Compressor reuses one set.
	withDecoders := codec.WithDecoders(decoders...)
	decode := codec.WithCompression(nil, withDecoders)
	if c == nil {
		return decode, nil, nil
	}

	encode := make(map[config.CompressionSettings]codec.Option)
	for s := range c.All() {
		if _, ok := encode[s]; ok || !s.Enabled {
			continue
		}

		k := compressionKey{algorithm: s.Algorithm}
		if s.Algorithm == config.CompressionZstd {
			k.level = s.Level
		}

		cmp, err := cs.get(k)
		if err != nil {
			return nil, nil, err
		}

		encode[s] = codec.WithCompression(cmp, withDecoders, codec.WithMinBytes(s.MinBytes))
	}

	return decode, encode, nil
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
