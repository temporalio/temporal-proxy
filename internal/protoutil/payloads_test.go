package protoutil_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/known/structpb"

	"github.com/temporalio/temporal-proxy/internal/protoutil"
	_ "github.com/temporalio/temporal-proxy/internal/services"
)

func descriptorFor(t *testing.T, name string) protoreflect.MessageDescriptor {
	t.Helper()

	d, err := protoregistry.GlobalFiles.FindDescriptorByName(protoreflect.FullName(name))
	require.NoError(t, err)

	md, ok := d.(protoreflect.MessageDescriptor)
	require.True(t, ok, "%q is not a message", name)
	return md
}

func TestCarriesPayloads(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		message string
		want    bool
	}{
		{
			name:    "a request carrying workflow input reaches Payload",
			message: "temporal.api.workflowservice.v1.StartWorkflowExecutionRequest",
			want:    true,
		},
		{
			name:    "Payload itself trivially carries payloads",
			message: "temporal.api.common.v1.Payload",
			want:    true,
		},
		{
			name:    "a request with no payload fields does not",
			message: "temporal.api.workflowservice.v1.GetSystemInfoRequest",
			want:    false,
		},
		{
			name:    "reflection messages do not",
			message: "grpc.reflection.v1.ServerReflectionRequest",
			want:    false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tt.want, protoutil.CarriesPayloads(descriptorFor(t, tt.message)))
		})
	}
}

func TestCarriesPayloadsTerminatesOnCyclicTypes(t *testing.T) {
	t.Parallel()

	// google.protobuf.Struct contains Value, which contains Struct. A walk
	// without a visited set never returns.
	md := (&structpb.Struct{}).ProtoReflect().Descriptor()
	require.False(t, protoutil.CarriesPayloads(md))
}

// TestCarriesPayloadsSeenGuardDoesNotMaskReachablePayload pins the ordering
// invariant the seen guard depends on. A: field 1 -> B, field 2 -> Payload.
// B: field 1 -> A. S: field 1 -> B. Walking A visits the back edge to B
// before the sibling field that reaches Payload directly, so if marking a
// type before its fields settled were unsafe, B (and transitively S) would
// come back false even though both can reach Payload through A.
func TestCarriesPayloadsSeenGuardDoesNotMaskReachablePayload(t *testing.T) {
	t.Parallel()

	messages := cyclicScratchMessages(t)
	for _, name := range []string{"A", "B", "S"} {
		md, ok := messages[name]
		require.True(t, ok, "missing synthetic message %q", name)
		require.True(t, protoutil.CarriesPayloads(md), "%s should reach Payload", md.FullName())
	}
}

// TestCarriesPayloadsFollowsMapValues pins the map-valued field branch of the
// walk on its own: Memo's only field is a map<string, Payload>, with no
// singular or repeated message field alongside it that could reach Payload
// instead.
func TestCarriesPayloadsFollowsMapValues(t *testing.T) {
	t.Parallel()

	md := descriptorFor(t, "temporal.api.common.v1.Memo")
	require.True(t, protoutil.CarriesPayloads(md))
}

// cyclicScratchMessages builds a synthetic scratch.v1 file with a back edge
// between A and B, plus S reaching the cycle from outside it, and returns
// each message descriptor by name. Building it via protodesc.NewFile against
// protoregistry.GlobalFiles lets it reference the real Payload type without
// forking or vendoring any Temporal proto.
func cyclicScratchMessages(t *testing.T) map[string]protoreflect.MessageDescriptor {
	t.Helper()

	messageField := func(name string, number int32, typeName string) *descriptorpb.FieldDescriptorProto {
		return &descriptorpb.FieldDescriptorProto{
			Name:     new(name),
			Number:   new(number),
			Label:    descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
			Type:     descriptorpb.FieldDescriptorProto_TYPE_MESSAGE.Enum(),
			TypeName: new(typeName),
		}
	}

	fdProto := &descriptorpb.FileDescriptorProto{
		Name:       new("scratch/v1/cycle.proto"),
		Package:    new("scratch.v1"),
		Syntax:     new("proto3"),
		Dependency: []string{"temporal/api/common/v1/message.proto"},
		MessageType: []*descriptorpb.DescriptorProto{
			{
				Name: new("A"),
				Field: []*descriptorpb.FieldDescriptorProto{
					messageField("b", 1, ".scratch.v1.B"),
					messageField("payload", 2, ".temporal.api.common.v1.Payload"),
				},
			},
			{
				Name:  new("B"),
				Field: []*descriptorpb.FieldDescriptorProto{messageField("a", 1, ".scratch.v1.A")},
			},
			{
				Name:  new("S"),
				Field: []*descriptorpb.FieldDescriptorProto{messageField("b", 1, ".scratch.v1.B")},
			},
		},
	}

	fd, err := protodesc.NewFile(fdProto, protoregistry.GlobalFiles)
	require.NoError(t, err)

	out := make(map[string]protoreflect.MessageDescriptor)
	msgs := fd.Messages()
	for i := 0; i < msgs.Len(); i++ {
		md := msgs.Get(i)
		out[string(md.Name())] = md
	}

	return out
}
