package protoutil

import (
	"testing"

	"github.com/stretchr/testify/require"
	namespacepb "go.temporal.io/api/namespace/v1"
	nexuspb "go.temporal.io/api/nexus/v1"
	"go.temporal.io/api/workflowservice/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// TestPlanPicksWriterPerField pins which writer each kind of namespace field
// gets: the suffix rule and NamespaceInfo.name rewrite a name, a Nexus link
// rewrites the namespace inside its URL, and a "*namespace_id" or other string
// field gets none.
func TestPlanPicksWriterPerField(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		msg  proto.Message
		want map[protoreflect.Name]fieldWriter
	}{
		{
			name: "suffix rule",
			msg:  &workflowservice.StartWorkflowExecutionRequest{},
			want: map[protoreflect.Name]fieldWriter{"namespace": nameField{}},
		},
		{
			name: "explicit name entry",
			msg:  &namespacepb.NamespaceInfo{},
			want: map[protoreflect.Name]fieldWriter{"name": nameField{}},
		},
		{
			name: "Nexus link URL",
			msg:  &nexuspb.Link{},
			want: map[protoreflect.Name]fieldWriter{"url": linkURLField{}},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			plan := buildPlan(tc.msg.ProtoReflect().Descriptor(), map[protoreflect.FullName]*nsPlan{})

			got := map[protoreflect.Name]fieldWriter{}
			for _, w := range plan.writers {
				got[w.Field().Name()] = w
			}

			require.Len(t, got, len(tc.want), "writers: %v", got)
			for field, want := range tc.want {
				require.Contains(t, got, field)
				require.IsType(t, want, got[field], "writer for %s", field)
			}
		})
	}
}
