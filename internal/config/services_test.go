package config_test

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/descriptorpb"

	"github.com/temporalio/temporal-proxy/internal/config"
	"github.com/temporalio/temporal-proxy/internal/services"
)

// validConfig returns the smallest configuration that passes Validate, so a
// case can vary one field and attribute any failure to that field.
func validConfig() *config.Config {
	return &config.Config{
		Listen:  config.ListenConfig{HostPort: "127.0.0.1:7233"},
		Routing: config.Routing{DefaultUpstream: "up"},
		Upstreams: []config.Upstream{{
			Name:   "up",
			Listen: config.ListenConfig{HostPort: "127.0.0.1:7234"},
		}},
	}
}

func TestEnabledServicesFallsBackToDefault(t *testing.T) {
	t.Parallel()

	cfg := validConfig()
	require.Equal(t, services.Default(), cfg.EnabledServices())
}

func TestEnabledServicesUsesConfiguredList(t *testing.T) {
	t.Parallel()

	cfg := validConfig()
	cfg.AllowedServices = []string{services.WorkflowService, services.Reflection}
	require.Equal(t, []string{services.WorkflowService, services.Reflection}, cfg.EnabledServices())
}

func TestValidateRejectsUnknownService(t *testing.T) {
	t.Parallel()

	cfg := validConfig()
	cfg.AllowedServices = []string{"temporal.api.nope.v1.NopeService"}

	err := cfg.Validate()
	require.Error(t, err)
	require.Contains(t, err.Error(), "temporal.api.nope.v1.NopeService")
	require.Contains(t, err.Error(), services.WorkflowService,
		"the failure must list what is linked so the fix is obvious")
}

func TestValidateAcceptsKnownServices(t *testing.T) {
	t.Parallel()

	cfg := validConfig()
	cfg.AllowedServices = services.Known()
	require.NoError(t, cfg.Validate())
}

// TestValidateRejectsRegisteredButUnforwardableService proves the guard checks
// forwardability, not mere resolvability. grpc.health.v1.Health resolves via
// services.Resolve (its descriptors are linked in transitively, through
// go.temporal.io/api's dependency on grpc-gateway, which imports it), but it is
// not in services.All(): the proxy answers health locally and never forwards
// it. Before this test, a config naming it passed validation and did nothing.
func TestValidateRejectsRegisteredButUnforwardableService(t *testing.T) {
	t.Parallel()

	const health = "grpc.health.v1.Health"
	_, err := services.Resolve(health)
	require.NoError(t, err, "this test's premise is that the name resolves despite not being forwardable")

	cfg := validConfig()
	cfg.AllowedServices = []string{health}

	verr := cfg.Validate()
	require.Error(t, verr)
	require.Contains(t, verr.Error(), health)
	require.Contains(t, verr.Error(), "not forwardable",
		"the message must say the name is unforwardable, not merely unresolvable")
}

// TestValidateReportsEveryUnresolvableService proves an operator with several
// typos sees all of them in one failure rather than fixing them one restart at
// a time.
func TestValidateReportsEveryUnresolvableService(t *testing.T) {
	t.Parallel()

	cfg := validConfig()
	cfg.AllowedServices = []string{"temporal.api.nope.v1.NopeService", "temporal.api.oops.v1.OopsService"}

	err := cfg.Validate()
	require.Error(t, err)
	require.Contains(t, err.Error(), "temporal.api.nope.v1.NopeService")
	require.Contains(t, err.Error(), "temporal.api.oops.v1.OopsService")
}

func TestValidateRejectsDuplicateServices(t *testing.T) {
	t.Parallel()

	cfg := validConfig()
	cfg.AllowedServices = []string{services.WorkflowService, services.WorkflowService}

	err := cfg.Validate()
	require.Error(t, err)
	require.Contains(t, err.Error(), "duplicate")
}

func TestLoadParsesAllowedServices(t *testing.T) {
	t.Parallel()

	cfg, err := config.Load(strings.NewReader(`
hostPort: 127.0.0.1:7233
allowedServices:
  - temporal.api.workflowservice.v1.WorkflowService
routing:
  default: up
upstreams:
  - name: up
    hostPort: 127.0.0.1:7234
`))
	require.NoError(t, err)
	require.Equal(t, []string{services.WorkflowService}, cfg.AllowedServices)
}

// uncoverablePayloadDescriptor builds a FileDescriptorProto for a synthetic
// "Scratch" service outside the temporal.api. tree whose request carries a
// Payload, which is the shape the guard must refuse. AdminService is the real
// example, but it lives in go.temporal.io/server and this package does not
// depend on it.
//
// fileName and pkg are parameters, not constants, because two tests need
// distinct descriptors: one built transiently for direct predicate testing,
// and one registered into the process-global proto registry so
// services.Resolve can find it. Reusing a file name or package between them
// (or with anything else in the module) would collide.
func uncoverablePayloadDescriptor(fileName, pkg string) *descriptorpb.FileDescriptorProto {
	return &descriptorpb.FileDescriptorProto{
		Name:       new(fileName),
		Package:    new(pkg),
		Syntax:     new("proto3"),
		Dependency: []string{"temporal/api/common/v1/message.proto"},
		MessageType: []*descriptorpb.DescriptorProto{
			{
				Name: new("Req"),
				Field: []*descriptorpb.FieldDescriptorProto{{
					Name:     new("payload"),
					Number:   proto.Int32(1),
					Label:    descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
					Type:     descriptorpb.FieldDescriptorProto_TYPE_MESSAGE.Enum(),
					TypeName: new(".temporal.api.common.v1.Payload"),
					JsonName: new("payload"),
				}},
			},
			{Name: new("Resp")},
		},
		Service: []*descriptorpb.ServiceDescriptorProto{{
			Name: new("Scratch"),
			Method: []*descriptorpb.MethodDescriptorProto{{
				Name:       new("Do"),
				InputType:  new("." + pkg + ".Req"),
				OutputType: new("." + pkg + ".Resp"),
			}},
		}},
	}
}

// uncoverableService builds a descriptor for a service outside the
// temporal.api. tree whose request carries a Payload, without registering it
// into the process-global proto registry. It is only ever resolved directly
// (via the returned value), never through services.Resolve, so it does not
// need registerUncoverableService's process-global side effect.
func uncoverableService(t *testing.T) protoreflect.ServiceDescriptor {
	t.Helper()

	fdp := uncoverablePayloadDescriptor("internal/config/uncoverable_test.proto", "scratch.v1")
	fd, err := protodesc.NewFile(fdp, protoregistry.GlobalFiles)
	require.NoError(t, err)

	return fd.Services().Get(0)
}

// registerUncoverableService builds a synthetic uncoverable service like
// uncoverableService, but additionally registers its file into
// protoregistry.GlobalFiles so services.Resolve (and, through it,
// Config.Validate) can find it by name. It returns the service's fully
// qualified name.
//
// This mutates a process-global registry, so the caller must not run this
// test with t.Parallel(): a concurrent registration under the same file name
// or package would either collide outright or make the test's outcome depend
// on interleaving with unrelated tests. The file path and package here are
// deliberately distinct from uncoverableService's (and from anything else in
// the module) so this is the only place that ever registers them.
func registerUncoverableService(t *testing.T) string {
	t.Helper()

	const pkg = "scratch.registered.v1"

	fdp := uncoverablePayloadDescriptor("internal/config/uncoverable_registered_test.proto", pkg)
	fd, err := protodesc.NewFile(fdp, protoregistry.GlobalFiles)
	require.NoError(t, err)
	require.NoError(t, protoregistry.GlobalFiles.RegisterFile(fd))

	return pkg + ".Scratch"
}

func TestEncryptionGuardRefusesUncoverableService(t *testing.T) {
	t.Parallel()

	require.True(t, config.EncryptionBlindSpot(uncoverableService(t)),
		"a payload-carrying service outside temporal.api. is invisible to the payload visitor")
}

func TestEncryptionGuardAllowsCoveredAndPayloadFreeServices(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		service string
	}{
		{name: "workflow service is inside the api tree", service: services.WorkflowService},
		{name: "operator service is inside the api tree", service: services.OperatorService},
		{name: "reflection carries no payloads at all", service: services.Reflection},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			sd, err := services.Resolve(tt.service)
			require.NoError(t, err)
			require.False(t, config.EncryptionBlindSpot(sd))
		})
	}
}

func TestValidateAllowsCoverableServicesWhenEncrypting(t *testing.T) {
	t.Parallel()

	cfg := validConfig()
	cfg.Encryption.Enabled = true
	cfg.Encryption.Default = &config.KeyPolicy{
		URI:         mustURL(t, "awskms://alias/primary"),
		Duration:    time.Hour,
		RenewBefore: 10 * time.Minute,
	}
	cfg.AllowedServices = services.Known()

	// Every linked service is coverable, so the guard stays quiet.
	require.NoError(t, cfg.Validate())
}

func TestValidateRefusesUncoverablePayloadsWhenEncrypting(t *testing.T) {
	// registerUncoverableService mutates protoregistry.GlobalFiles, a
	// process-global registry shared by every test in this binary, so this
	// test must not run with t.Parallel(): interleaving its registration with
	// another test's use of the registry is exactly the kind of shared-state
	// race t.Parallel() would introduce for no benefit.
	name := registerUncoverableService(t)

	cfg := validConfig()
	cfg.Encryption.Enabled = true
	cfg.Encryption.Default = &config.KeyPolicy{
		URI:         mustURL(t, "awskms://alias/primary"),
		Duration:    time.Hour,
		RenewBefore: 10 * time.Minute,
	}
	cfg.AllowedServices = []string{name}

	// If encryptionCoverageRule were ever dropped from serviceRules, this is
	// the only test that would catch it: the predicate tests only exercise
	// EncryptionBlindSpot directly, and the "allows" test above only proves
	// the guard stays quiet, which would also be true of a guard that never
	// ran at all.
	err := cfg.Validate()
	require.Error(t, err)
	require.Contains(t, err.Error(), name)
}
