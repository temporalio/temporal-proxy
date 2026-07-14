package protoutil_test

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/api/workflowservice/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"

	"github.com/temporalio/temporal-proxy/internal/protoutil"
)

const (
	wfService    = "temporal.api.workflowservice.v1.WorkflowService"
	startMethod  = "/" + wfService + "/StartWorkflowExecution"
	systemMethod = "/" + wfService + "/GetSystemInfo"
)

type (
	errFiles struct{ err error }
	errTypes struct{ err error }
)

func TestExtractorNamespace(t *testing.T) {
	t.Parallel()

	start := mustMarshal(t, &workflowservice.StartWorkflowExecutionRequest{Namespace: "ns-1"})
	system := mustMarshal(t, &workflowservice.GetSystemInfoRequest{})

	tests := []struct {
		name    string
		method  string
		payload []byte
		want    string
	}{
		{name: "namespaced request", method: startMethod, payload: start, want: "ns-1"},
		{name: "system request has no namespace", method: systemMethod, payload: system, want: ""},
		{name: "unknown service", method: "/nope.v1.Service/Method", payload: start, want: ""},
		{name: "name resolves to a non-service", method: "/temporal.api.workflowservice.v1.StartWorkflowExecutionRequest/Method", payload: start, want: ""},
		{name: "unknown method on real service", method: "/" + wfService + "/NoSuchMethod", payload: start, want: ""},
		{name: "malformed method name", method: "no-slashes", payload: start, want: ""},
		{name: "garbage payload", method: startMethod, payload: []byte{0xff, 0xff, 0xff}, want: ""},
		{name: "empty namespace", method: startMethod, payload: mustMarshal(t, &workflowservice.StartWorkflowExecutionRequest{}), want: ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ex := protoutil.NewExtractor(protoregistry.GlobalFiles, protoregistry.GlobalTypes)
			require.Equal(t, tt.want, ex.Namespace(tt.method, tt.payload))
		})
	}
}

func TestExtractorCaching(t *testing.T) {
	t.Parallel()

	start := mustMarshal(t, &workflowservice.StartWorkflowExecutionRequest{Namespace: "ns-1"})

	t.Run("resolved type is reused", func(t *testing.T) {
		t.Parallel()
		ex := protoutil.NewExtractor(protoregistry.GlobalFiles, protoregistry.GlobalTypes)
		require.Equal(t, "ns-1", ex.Namespace(startMethod, start))
		require.Equal(t, "ns-1", ex.Namespace(startMethod, start), "second lookup uses the cached type")
	})

	t.Run("unknown method stays empty on repeat", func(t *testing.T) {
		t.Parallel()
		ex := protoutil.NewExtractor(protoregistry.GlobalFiles, protoregistry.GlobalTypes)
		require.Equal(t, "", ex.Namespace("/nope.v1.Service/Method", start))
		require.Equal(t, "", ex.Namespace("/nope.v1.Service/Method", start), "cached nil miss must not panic")
	})
}

func TestExtractorConsultsInjectedSources(t *testing.T) {
	t.Parallel()

	start := mustMarshal(t, &workflowservice.StartWorkflowExecutionRequest{Namespace: "ns-1"})

	t.Run("files error yields empty", func(t *testing.T) {
		t.Parallel()
		ex := protoutil.NewExtractor(errFiles{err: errors.New("boom")}, protoregistry.GlobalTypes)
		require.Equal(t, "", ex.Namespace(startMethod, start))
	})

	t.Run("types error yields empty", func(t *testing.T) {
		t.Parallel()
		// Real files resolve the service and method; the injected types then
		// fails, so extraction stops there. This proves types is consulted.
		ex := protoutil.NewExtractor(protoregistry.GlobalFiles, errTypes{err: errors.New("boom")})
		require.Equal(t, "", ex.Namespace(startMethod, start))
	})
}

func (f errFiles) FindDescriptorByName(protoreflect.FullName) (protoreflect.Descriptor, error) {
	return nil, f.err
}

func (t errTypes) FindMessageByName(protoreflect.FullName) (protoreflect.MessageType, error) {
	return nil, t.err
}

func mustMarshal(t *testing.T, m proto.Message) []byte {
	t.Helper()
	b, err := proto.Marshal(m)
	require.NoError(t, err)
	return b
}
