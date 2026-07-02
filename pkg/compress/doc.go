// Package compress provides byte-slice compressors used to shrink payloads
// as they pass through the proxy.
//
// Each compressor ([Snappy], [Zstd], [LZ4]) exposes the same shape: an Encoding
// method returning the algorithm's name (lz4, zstd, or snappy), and
// Compress/Decompress methods that operate on whole in-memory buffers. Both
// treat their input as read-only and always return a newly allocated result.
// Decompressed output is capped (see [WithMaxDecodedSize]), since compressed
// payloads may come from untrusted writers.
package compress
