package codec_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/api/common/v1"
	failurepb "go.temporal.io/api/failure/v1"
	"google.golang.org/protobuf/proto"

	"go.temporal.io/proxy/pkg/codec"
)

func TestEncodeFailure(t *testing.T) {
	t.Parallel()

	f := &failurepb.Failure{Message: "boom", StackTrace: "at x"}
	require.NoError(t, codec.EncodeFailure(f))

	require.Equal(t, "Encoded failure", f.GetMessage())
	require.Empty(t, f.GetStackTrace())

	// Golden values produced by the Temporal SDK's EncodeCommonFailureAttributes
	// (v1.49.0, default data converter). An SDK worker decodes these with
	// DecodeCommonFailureAttributes, so any drift here leaves it showing
	// "Encoded failure" instead of the real message.
	attrs := f.GetEncodedAttributes()
	require.Equal(t, map[string][]byte{"encoding": []byte("json/plain")}, attrs.GetMetadata())
	require.JSONEq(t, `{"message":"boom","stack_trace":"at x"}`, string(attrs.GetData()))
}

// A worker running the SDK with EncodeCommonAttributes has already moved the
// text, so it is left as the SDK wrote it rather than wrapped again.
func TestEncodeFailureKeepsEncodedAttributes(t *testing.T) {
	t.Parallel()

	attrs := testPayload("binary/custom", "sealed by a client")
	f := &failurepb.Failure{Message: "Encoded failure", EncodedAttributes: proto.Clone(attrs).(*common.Payload)}
	require.NoError(t, codec.EncodeFailure(f))

	require.Equal(t, "Encoded failure", f.GetMessage())
	require.True(t, proto.Equal(attrs, f.GetEncodedAttributes()))
}

// A failure RestoreFailure filled in keeps its text only in the attributes, so
// a Worker sending it back unchanged does not send that text in plaintext.
func TestEncodeFailureClearsRestoredText(t *testing.T) {
	t.Parallel()

	attrs := testPayload("json/plain", `{"message":"boom","stack_trace":"at x"}`)
	f := &failurepb.Failure{EncodedAttributes: proto.Clone(attrs).(*common.Payload)}
	require.NoError(t, codec.RestoreFailure(f))
	require.Equal(t, "boom", f.GetMessage())

	require.NoError(t, codec.EncodeFailure(f))

	require.Equal(t, "Encoded failure", f.GetMessage())
	require.Empty(t, f.GetStackTrace())
	require.True(t, proto.Equal(attrs, f.GetEncodedAttributes()))
}

// Causes are separate failures, visited on their own, so encoding one leaves
// its cause alone.
func TestEncodeFailureLeavesCause(t *testing.T) {
	t.Parallel()

	f := &failurepb.Failure{Message: "outer", Cause: &failurepb.Failure{Message: "inner"}}
	require.NoError(t, codec.EncodeFailure(f))

	require.Equal(t, "inner", f.GetCause().GetMessage())
	require.Nil(t, f.GetCause().GetEncodedAttributes())
}

// Any encoded attributes stand in for the text, whatever their encoding, so the
// text beside them is cleared. A failure with none keeps its text.
func TestClearFailureText(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		attrs   *common.Payload
		message string
		stack   string
	}{
		{
			name:    "restored from the SDK's JSON",
			attrs:   testPayload("json/plain", `{"message":"boom","stack_trace":"at x"}`),
			message: "Encoded failure",
		},
		{
			name:    "re-encoded by a client codec",
			attrs:   testPayload("binary/custom", "sealed by a client"),
			message: "Encoded failure",
		},
		{
			name:    "no attributes",
			message: "boom",
			stack:   "at x",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			var attrs *common.Payload
			if tc.attrs != nil {
				attrs = proto.Clone(tc.attrs).(*common.Payload)
			}

			f := &failurepb.Failure{
				Message:           "boom",
				StackTrace:        "at x",
				EncodedAttributes: attrs,
				Cause:             &failurepb.Failure{Message: "inner", EncodedAttributes: testPayload("json/plain", "{}")},
			}
			codec.ClearFailureText(f)

			require.Equal(t, tc.message, f.GetMessage())
			require.Equal(t, tc.stack, f.GetStackTrace())
			require.True(t, proto.Equal(tc.attrs, f.GetEncodedAttributes()))
			require.Equal(t, "inner", f.GetCause().GetMessage())
		})
	}
}

// A client reading through the proxy, such as the Web UI or CLI, sees the text
// where it expects it. The attributes stay too, so an SDK client decoding them
// gets the same values.
func TestRestoreFailure(t *testing.T) {
	t.Parallel()

	attrs := testPayload("json/plain", `{"message":"boom","stack_trace":"at x"}`)
	f := &failurepb.Failure{Message: "Encoded failure", EncodedAttributes: proto.Clone(attrs).(*common.Payload)}
	require.NoError(t, codec.RestoreFailure(f))

	require.Equal(t, "boom", f.GetMessage())
	require.Equal(t, "at x", f.GetStackTrace())
	require.True(t, proto.Equal(attrs, f.GetEncodedAttributes()))
}

func TestRestoreFailureKeepsUnreadableAttributes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		attrs *common.Payload
	}{
		{
			// Readable JSON, so only the encoding keeps it from being restored.
			name:  "sealed by a client codec",
			attrs: testPayload("binary/custom", `{"message":"boom","stack_trace":"at x"}`),
		},
		{
			name:  "not the SDK's JSON",
			attrs: testPayload("json/plain", "not json"),
		},
		{
			name: "no attributes",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			f := &failurepb.Failure{Message: "Encoded failure", EncodedAttributes: tc.attrs}
			require.NoError(t, codec.RestoreFailure(f))

			require.Equal(t, "Encoded failure", f.GetMessage())
			require.True(t, proto.Equal(tc.attrs, f.GetEncodedAttributes()))
		})
	}
}
