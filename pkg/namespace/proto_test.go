package namespace_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	_ "go.temporal.io/api/operatorservice/v1"
	_ "go.temporal.io/api/workflowservice/v1"
	_ "go.temporal.io/server/api/adminservice/v1"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"

	"github.com/temporalio/temporal-proxy/pkg/namespace"
)

// Services to walk to ensure we're not missing any namespace field coverage.
// Don't forget the _ import!
//
// NB: We should consider parameterizing this by version so we could do
// forward/backward compatibility tests.
var servicesToTest = []protoreflect.FullName{
	"temporal.api.workflowservice.v1.WorkflowService",
	"temporal.api.operatorservice.v1.OperatorService",
	"temporal.server.api.adminservice.v1.AdminService",
}

// TestNamespaceFieldCoverage walks every request and response message reachable
// from the mentioned services and asserts that every string field whose proto
// name carries namespace meaning is recognized by the translator. When Temporal
// adds a new namespace field, this test fails with the exact field name to
// investigate.
func TestNamespaceFieldCoverage(t *testing.T) {
	t.Parallel()

	visited := map[protoreflect.FullName]bool{}
	var missed []string

	for _, svc := range servicesToTest {
		desc, err := protoregistry.GlobalFiles.FindDescriptorByName(svc)
		require.NoError(t, err, "service %q not registered; ensure its package is imported", svc)

		sd, ok := desc.(protoreflect.ServiceDescriptor)
		require.True(t, ok, "descriptor %q is not a service descriptor", svc)

		methods := sd.Methods()
		for i := range methods.Len() {
			m := methods.Get(i)
			scanMessageDescriptor(m.Input(), visited, &missed)
			scanMessageDescriptor(m.Output(), visited, &missed)
		}
	}

	require.Empty(t, missed,
		"unrecognized namespace-bearing string fields found:\n  %s\n\n"+
			"Each of these is a string field whose proto name contains \"namespace\" and is not\n"+
			"in the translator's allowlist. Add it to namespaceNameFields in fields.go (if the\n"+
			"field name globally denotes a namespace name), or to messageScopedNamespaceFields\n"+
			"(if only certain message types use this field as a namespace name).",
		strings.Join(missed, "\n  "))
}

func scanMessageDescriptor(md protoreflect.MessageDescriptor, visited map[protoreflect.FullName]bool, missed *[]string) {
	if visited[md.FullName()] {
		return
	}
	visited[md.FullName()] = true

	fields := md.Fields()
	for i := range fields.Len() {
		fd := fields.Get(i)

		if fd.IsMap() {
			if mv := fd.MapValue(); mv.Kind() == protoreflect.MessageKind || mv.Kind() == protoreflect.GroupKind {
				scanMessageDescriptor(mv.Message(), visited, missed)
			}
			continue
		}
		if fd.Kind() == protoreflect.MessageKind || fd.Kind() == protoreflect.GroupKind {
			scanMessageDescriptor(fd.Message(), visited, missed)
			continue
		}
		if fd.Kind() != protoreflect.StringKind {
			continue
		}

		field := string(fd.Name())
		if !strings.Contains(field, "namespace") {
			continue
		}

		msg := string(md.FullName())
		if namespace.IsKnownProtoField(field, msg) {
			continue
		}

		*missed = append(*missed, msg+"."+field)
	}
}
