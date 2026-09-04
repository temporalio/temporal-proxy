package services

import (
	"fmt"
	"slices"

	// Blank imports register each forwardable service's descriptors with
	// protoregistry.GlobalFiles. Without them Resolve fails and the forwarder
	// cannot type a request, so these imports are load-bearing rather than
	// incidental.
	_ "go.temporal.io/api/operatorservice/v1"
	_ "go.temporal.io/api/workflowservice/v1"
	_ "go.temporal.io/cloud-sdk/api/cloudservice/v1"
	_ "google.golang.org/grpc/reflection/grpc_reflection_v1"
	_ "google.golang.org/grpc/reflection/grpc_reflection_v1alpha"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
)

const (
	// WorkflowService is the Temporal service every client, worker, and UI uses.
	WorkflowService = "temporal.api.workflowservice.v1.WorkflowService"

	// OperatorService backs `temporal operator ...` and the UI's search
	// attribute and Nexus endpoint views.
	OperatorService = "temporal.api.operatorservice.v1.OperatorService"

	// CloudService backs saas-temporal API's for management of the hosted solution.
	CloudService = "temporal.api.cloud.cloudservice.v1.CloudService"

	// Reflection is the current gRPC server reflection service.
	Reflection = "grpc.reflection.v1.ServerReflection"

	// ReflectionV1Alpha is the superseded reflection service. Clients such as
	// grpcurl probe v1 and fall back to it, so allowing one allows both.
	ReflectionV1Alpha = "grpc.reflection.v1alpha.ServerReflection"
)

// aliases maps a service name to the additional names allowing it implies.
// Callers name the service they mean; Expand supplies the compatibility
// spellings so configuration does not have to.
var aliases = map[string][]string{
	Reflection: {ReflectionV1Alpha},
}

// Default returns the services exposed when configuration names none: the two a
// normal client, worker, CLI, or UI needs. Reflection is excluded because
// service discovery is opt-in.
func Default() []string {
	return []string{WorkflowService, OperatorService, CloudService}
}

// Known returns every service the proxy can forward, which is every service
// whose descriptors this package links in. It is the universe configuration may
// select from, and the set the namespace completeness guard audits.
func Known() []string {
	return []string{WorkflowService, OperatorService, CloudService, Reflection}
}

// All returns every forwardable service name, including each service's
// compatibility alias: the full set a name might need to be checked against, as
// opposed to Known's plain spelling of it. A caller checking membership against
// "the complete set of names that count as forwardable" should use All.
func All() []string {
	return expand(Known())
}

// Resolve returns the descriptor for the named service, failing when the name
// is not registered (its package is not linked in) or names something other
// than a service.
func Resolve(name string) (protoreflect.ServiceDescriptor, error) {
	d, err := protoregistry.GlobalFiles.FindDescriptorByName(protoreflect.FullName(name))
	if err != nil {
		return nil, fmt.Errorf("service %q is not registered: %w", name, err)
	}

	sd, ok := d.(protoreflect.ServiceDescriptor)
	if !ok {
		return nil, fmt.Errorf("%q is not a service", name)
	}

	return sd, nil
}

// expand returns names plus the alias of every name that has one, with
// duplicates removed. Order is not significant to callers, which build sets.
func expand(names []string) []string {
	out := slices.Clone(names)
	for _, name := range names {
		for _, alias := range aliases[name] {
			if !slices.Contains(out, alias) {
				out = append(out, alias)
			}
		}
	}

	return out
}
