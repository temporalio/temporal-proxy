package codec

import (
	"encoding/json"
	"fmt"

	"go.temporal.io/api/common/v1"
	"go.temporal.io/api/failure/v1"
)

// encodedFailureMessage replaces the message of a failure whose attributes were
// moved into a payload. It is the placeholder the Temporal SDK writes.
const encodedFailureMessage = "Encoded failure"

// encodedFailure is the JSON shape the Temporal SDK's
// EncodeCommonFailureAttributes writes and DecodeCommonFailureAttributes reads.
type encodedFailure struct {
	Message    string `json:"message"`
	StackTrace string `json:"stack_trace"`
}

// EncodeFailure moves f's message and stack trace into its encoded attributes,
// in the form the Temporal SDK writes, so a payload codec run afterwards seals
// them and an SDK client restores them on read. A failure that already has
// encoded attributes is left alone, and f's cause is not visited.
func EncodeFailure(f *failure.Failure) error {
	if f.GetEncodedAttributes() != nil {
		return nil
	}

	data, err := json.Marshal(encodedFailure{Message: f.GetMessage(), StackTrace: f.GetStackTrace()})
	if err != nil {
		return fmt.Errorf("codec: failed to encode failure attributes: %w", err)
	}

	f.EncodedAttributes = &common.Payload{
		Metadata: map[string][]byte{MetadataEncoding: []byte("json/plain")},
		Data:     data,
	}
	f.Message = encodedFailureMessage
	f.StackTrace = ""

	return nil
}

// RestoreFailure copies the message and stack trace in f's encoded attributes
// back onto f, so a client that does not decode them still sees the text. Run it
// after a payload codec has opened the attributes. The attributes are kept, and
// f's cause is not visited.
//
// Attributes that are not the SDK's JSON, such as ones a client sealed with its
// own codec, are left alone. Like the SDK's DecodeCommonFailureAttributes, that
// is not an error: failing the call would cost the response for the sake of a
// message.
func RestoreFailure(f *failure.Failure) error {
	attrs := f.GetEncodedAttributes()
	if string(attrs.GetMetadata()[MetadataEncoding]) != "json/plain" {
		return nil
	}

	var ef encodedFailure
	if err := json.Unmarshal(attrs.GetData(), &ef); err != nil {
		return nil //nolint:nilerr // unreadable attributes keep the placeholder; see above
	}

	f.Message = ef.Message
	f.StackTrace = ef.StackTrace

	return nil
}
