package codec_test

import (
	"crypto/rand"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/api/common/v1"
	"google.golang.org/protobuf/proto"

	"go.temporal.io/proxy/pkg/codec"
	"go.temporal.io/proxy/pkg/compress"
)

// recordingObserver records what a Compressor reports, one string per call.
type recordingObserver struct {
	events    []string
	algorithm string
	in, out   int
	err       error
}

func TestCompressorRoundtrip(t *testing.T) {
	t.Parallel()

	snappy := newSnappy(t)
	original := []*common.Payload{compressiblePayload()}
	want := proto.Clone(original[0]).(*common.Payload)

	compressed, err := codec.NewCompressor(snappy).Encode(original)
	require.NoError(t, err)
	require.Len(t, compressed, 1)

	md := compressed[0].Metadata
	require.Equal(t, codec.CompressionEncoding, string(md[codec.MetadataEncoding]))
	require.Equal(t, snappy.Encoding(), string(md[codec.MetadataCompressionAlgorithm]))
	require.Less(t, compressed[0].Size(), want.Size())

	restored, err := codec.NewCompressor(snappy).Decode(compressed)
	require.NoError(t, err)
	require.Len(t, restored, 1)
	require.True(t, proto.Equal(want, restored[0]), "payload did not round-trip")
}

func TestCompressorDecodePassesThroughForeignPayloads(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		payload *common.Payload
	}{
		{name: "not compressed", payload: testPayload("json/plain", `"hi"`)},
		{name: "no encoding", payload: &common.Payload{Data: []byte("raw")}},
		{
			// Something else using the same encoding name, without an algorithm
			// entry of ours.
			name:    "marker without algorithm",
			payload: testPayload(codec.CompressionEncoding, "not ours"),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			res, err := codec.NewCompressor(newSnappy(t)).Decode([]*common.Payload{tc.payload})
			require.NoError(t, err)
			require.Len(t, res, 1)
			require.Same(t, tc.payload, res[0])
		})
	}
}

func TestCompressorDecodeUsesRecordedAlgorithm(t *testing.T) {
	t.Parallel()

	zstd := newZstd(t)
	original := []*common.Payload{compressiblePayload()}
	want := proto.Clone(original[0]).(*common.Payload)

	compressed, err := codec.NewCompressor(zstd).Encode(original)
	require.NoError(t, err)

	// Compressing with snappy now doesn't strand what zstd compressed earlier.
	restored, err := codec.NewCompressor(newSnappy(t), codec.WithDecoders(zstd)).Decode(compressed)
	require.NoError(t, err)
	require.Len(t, restored, 1)
	require.True(t, proto.Equal(want, restored[0]), "payload did not round-trip")
}

func TestCompressorDecodeRejectsUnknownAlgorithm(t *testing.T) {
	t.Parallel()

	compressed, err := codec.NewCompressor(newZstd(t)).Encode([]*common.Payload{compressiblePayload()})
	require.NoError(t, err)

	// Handing compressed bytes back as if they were the payload would be worse
	// than failing the request.
	_, err = codec.NewCompressor(newSnappy(t)).Decode(compressed)
	require.ErrorContains(t, err, `unknown algorithm "zstd"`)
}

func TestCompressorEncodeMinBytes(t *testing.T) {
	t.Parallel()

	p := compressiblePayload()
	size := len(p.Data)

	tests := []struct {
		name       string
		minBytes   int
		compressed bool
	}{
		{name: "below threshold", minBytes: size + 1, compressed: false},
		{name: "at threshold", minBytes: size, compressed: true},
		{name: "above threshold", minBytes: size - 1, compressed: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			c := codec.NewCompressor(newSnappy(t), codec.WithMinBytes(tc.minBytes))
			res, err := c.Encode([]*common.Payload{p})
			require.NoError(t, err)
			require.Len(t, res, 1)

			if tc.compressed {
				require.Equal(t, codec.CompressionEncoding, string(res[0].Metadata[codec.MetadataEncoding]))
			} else {
				require.Same(t, p, res[0])
			}
		})
	}
}

func TestCompressorEncodeKeepsOriginalWhenNotSmaller(t *testing.T) {
	t.Parallel()

	// Random bytes don't compress, so framing and metadata make the result
	// bigger than what went in.
	data := make([]byte, 512)
	_, _ = rand.Read(data)
	p := testPayload("binary/plain", string(data))

	res, err := codec.NewCompressor(newSnappy(t)).Encode([]*common.Payload{p})
	require.NoError(t, err)
	require.Len(t, res, 1)
	require.Same(t, p, res[0])
}

func TestCompressorEncodeSkipsCompressedPayloads(t *testing.T) {
	t.Parallel()

	// Compressible data, so only the marker keeps it from being compressed again.
	p := compressiblePayload()
	p.Metadata[codec.MetadataEncoding] = []byte(codec.CompressionEncoding)

	res, err := codec.NewCompressor(newSnappy(t)).Encode([]*common.Payload{p})
	require.NoError(t, err)
	require.Len(t, res, 1)
	require.Same(t, p, res[0])
}

func TestCompressorDecodeOnly(t *testing.T) {
	t.Parallel()

	snappy := newSnappy(t)
	original := []*common.Payload{compressiblePayload()}
	want := proto.Clone(original[0]).(*common.Payload)

	compressed, err := codec.NewCompressor(snappy).Encode(original)
	require.NoError(t, err)

	// With compression turned off, nothing new is compressed but what was
	// compressed earlier still decodes.
	c := codec.NewCompressor(nil, codec.WithDecoders(snappy))

	p := compressiblePayload()
	res, err := c.Encode([]*common.Payload{p})
	require.NoError(t, err)
	require.Len(t, res, 1)
	require.Same(t, p, res[0])

	restored, err := c.Decode(compressed)
	require.NoError(t, err)
	require.Len(t, restored, 1)
	require.True(t, proto.Equal(want, restored[0]), "payload did not round-trip")
}

func TestCompressorDecodeReturnsInputWhenUnchanged(t *testing.T) {
	t.Parallel()

	in := []*common.Payload{testPayload("json/plain", `"a"`), testPayload("json/plain", `"b"`)}

	// Most responses carry nothing compressed, so passing them through must not
	// copy the slice.
	out, err := codec.NewCompressor(newSnappy(t)).Decode(in)
	require.NoError(t, err)
	require.Same(t, &in[0], &out[0])
}

func TestCompressorDecodeLeavesInputUntouched(t *testing.T) {
	t.Parallel()

	snappy := newSnappy(t)
	compressed, err := codec.NewCompressor(snappy).Encode([]*common.Payload{compressiblePayload()})
	require.NoError(t, err)

	plain := testPayload("json/plain", `"a"`)
	in := []*common.Payload{plain, compressed[0]}

	out, err := codec.NewCompressor(snappy).Decode(in)
	require.NoError(t, err)
	require.Len(t, out, 2)
	require.Same(t, plain, out[0])
	require.True(t, proto.Equal(compressiblePayload(), out[1]), "payload did not round-trip")

	// The caller's slice still holds what it handed over.
	require.Same(t, plain, in[0])
	require.Same(t, compressed[0], in[1])
}

func TestCompressorEncodeReturnsInputWhenUnchanged(t *testing.T) {
	t.Parallel()

	in := []*common.Payload{testPayload("json/plain", `"a"`), testPayload("json/plain", `"b"`)}

	out, err := codec.NewCompressor(newSnappy(t), codec.WithMinBytes(1024)).Encode(in)
	require.NoError(t, err)
	require.Same(t, &in[0], &out[0])
}

func TestCompressorEncodeLeavesInputUntouched(t *testing.T) {
	t.Parallel()

	small, big := testPayload("json/plain", `"a"`), compressiblePayload()
	in := []*common.Payload{small, big}

	out, err := codec.NewCompressor(newSnappy(t), codec.WithMinBytes(16)).Encode(in)
	require.NoError(t, err)
	require.Len(t, out, 2)
	require.Same(t, small, out[0])
	require.Equal(t, codec.CompressionEncoding, string(out[1].Metadata[codec.MetadataEncoding]))

	// The caller's slice still holds what it handed over.
	require.Same(t, small, in[0])
	require.Same(t, big, in[1])
}

// TestCompressorDecodeOnlyAllocations shows a decode-only Compressor built per
// request from a prebuilt WithDecoders option, then handed payloads that aren't
// compressed, costs at most the Compressor itself. Building the decoder map in
// NewCompressor or the option's closure fails it. AllocsPerRun counts every
// allocation in the process, so it can't run in parallel.
func TestCompressorDecodeOnlyAllocations(t *testing.T) {
	decoders := codec.WithDecoders(newSnappy(t), newZstd(t))
	in := []*common.Payload{testPayload("json/plain", `"a"`), testPayload("json/plain", `"b"`)}

	allocs := testing.AllocsPerRun(100, func() {
		_, _ = codec.NewCompressor(nil, decoders).Decode(in)
	})
	require.LessOrEqual(t, allocs, 1.0)
}

// TestCompressorObserver shows each payload reaches the observer once, as
// either an op or a skip with its reason. Dropping a skipped call, or reporting
// the not-smaller case only as a skip, fails it.
func TestCompressorObserver(t *testing.T) {
	t.Parallel()

	incompressible := make([]byte, 512)
	_, _ = rand.Read(incompressible)

	marked := compressiblePayload()
	marked.Metadata[codec.MetadataEncoding] = []byte(codec.CompressionEncoding)

	tests := []struct {
		name    string
		payload *common.Payload
		want    []string
	}{
		{name: "compressed", payload: compressiblePayload(), want: []string{"compress snappy <nil>"}},
		{name: "below minBytes", payload: testPayload("json/plain", `"a"`), want: []string{"skip below_min_bytes snappy"}},
		{name: "already compressed", payload: marked, want: []string{"skip already_compressed snappy"}},
		{
			// Compression ran and cost CPU, so it is an op as well as a skip.
			name:    "not smaller",
			payload: testPayload("binary/plain", string(incompressible)),
			want:    []string{"compress snappy <nil>", "skip not_smaller snappy"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			obs := &recordingObserver{}
			c := codec.NewCompressor(newSnappy(t), codec.WithMinBytes(16), codec.WithCompressionObserver(obs))
			_, err := c.Encode([]*common.Payload{tc.payload})
			require.NoError(t, err)
			require.Equal(t, tc.want, obs.events)
		})
	}
}

// TestCompressorObserverDecode shows a restored payload is observed with the
// sizes either side of the algorithm, and an unknown algorithm with the name the
// payload gave and an error matching ErrUnknownAlgorithm. Passing the sizes in
// the wrong order fails the first case.
func TestCompressorObserverDecode(t *testing.T) {
	t.Parallel()

	compressed, err := codec.NewCompressor(newZstd(t)).Encode([]*common.Payload{compressiblePayload()})
	require.NoError(t, err)

	t.Run("restored", func(t *testing.T) {
		t.Parallel()

		obs := &recordingObserver{}
		_, err := codec.NewCompressor(newZstd(t), codec.WithCompressionObserver(obs)).Decode(compressed)
		require.NoError(t, err)
		require.Equal(t, []string{"decompress zstd <nil>"}, obs.events)
		require.Less(t, obs.in, obs.out)
	})

	t.Run("unknown algorithm", func(t *testing.T) {
		t.Parallel()

		obs := &recordingObserver{}
		_, err := codec.NewCompressor(newSnappy(t), codec.WithCompressionObserver(obs)).Decode(compressed)
		require.ErrorIs(t, err, codec.ErrUnknownAlgorithm)
		require.Len(t, obs.events, 1)
		require.ErrorIs(t, obs.err, codec.ErrUnknownAlgorithm)
		require.Equal(t, "zstd", obs.algorithm)
	})
}

func (r *recordingObserver) Observe(op, algorithm string, in, out int, _ time.Duration, err error) {
	r.algorithm, r.in, r.out, r.err = algorithm, in, out, err
	r.events = append(r.events, fmt.Sprintf("%s %s %v", op, algorithm, err))
}

func (r *recordingObserver) Skipped(reason, algorithm string) {
	r.events = append(r.events, "skip "+reason+" "+algorithm)
}

// compressiblePayload returns a payload whose data compresses well.
func compressiblePayload() *common.Payload {
	return testPayload("json/plain", `"`+strings.Repeat("compress me ", 100)+`"`)
}

func newSnappy(t *testing.T) *compress.Snappy {
	t.Helper()

	c, err := compress.NewSnappy()
	require.NoError(t, err)

	return c
}

func newZstd(t *testing.T) *compress.Zstd {
	t.Helper()

	c, err := compress.NewZstd(3)
	require.NoError(t, err)

	return c
}
