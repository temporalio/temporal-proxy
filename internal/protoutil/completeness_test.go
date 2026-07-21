package protoutil

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	_ "go.temporal.io/api/workflowservice/v1"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
)

// completenessServices lists the gRPC services the guard checks. It should
// cover every service the proxy translates (see cmd/proxy). Each service's
// package must be blank-imported above so its descriptors resolve.
var completenessServices = []protoreflect.FullName{
	"temporal.api.workflowservice.v1.WorkflowService",
}

func TestRuleSetCoversServices(t *testing.T) {
	t.Parallel()

	seen := map[protoreflect.FullName]bool{}
	var offenders []string

	var walk func(md protoreflect.MessageDescriptor)
	walk = func(md protoreflect.MessageDescriptor) {
		if seen[md.FullName()] {
			return
		}
		seen[md.FullName()] = true

		fields := md.Fields()
		for i := 0; i < fields.Len(); i++ {
			fd := fields.Get(i)
			if suspiciousButUnclassified(md, fd) {
				offenders = append(offenders, string(md.FullName())+"."+string(fd.Name()))
			}

			if sub := childMessage(fd); sub != nil {
				walk(sub)
			}
		}
	}

	for _, name := range completenessServices {
		d, err := protoregistry.GlobalFiles.FindDescriptorByName(name)
		require.NoError(t, err, "service %q not registered; add a blank import for its package", name)

		sd, ok := d.(protoreflect.ServiceDescriptor)
		require.True(t, ok, "%q resolved to a non-service descriptor", name)

		methods := sd.Methods()
		for i := 0; i < methods.Len(); i++ {
			m := methods.Get(i)
			walk(m.Input())
			walk(m.Output())
		}
	}

	require.Empty(t, offenders,
		"these string fields look like namespace names but the rule set does not classify them; "+
			"either extend the suffix rule / namespaceNameOverrides, or confirm they are not namespaces:\n%s",
		strings.Join(offenders, "\n"))
}

// suspiciousButUnclassified reports whether fd looks like it could carry a
// namespace name yet isNamespaceName does not translate it. A string field is
// suspicious when its name mentions "namespace" (but is not a "*namespace_id"
// UUID) or when it is named "name" on a message type whose own name mentions
// "Namespace". Any such field must be either translated by the rule set or added
// to an explicit, reviewed exclusion.
func suspiciousButUnclassified(md protoreflect.MessageDescriptor, fd protoreflect.FieldDescriptor) bool {
	if fd.Kind() != protoreflect.StringKind || isNamespaceName(md, fd) {
		return false
	}

	name := string(fd.Name())
	if strings.Contains(name, "namespace") && !strings.HasSuffix(name, "namespace_id") {
		return true
	}

	return name == "name" && strings.Contains(strings.ToLower(string(md.Name())), "namespace")
}
