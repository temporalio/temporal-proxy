package protoutil_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/api/workflowservice/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoregistry"

	"github.com/temporalio/temporal-proxy/internal/protoutil"
	"github.com/temporalio/temporal-proxy/internal/testutils"
)

func benchMarshal(b *testing.B, m proto.Message) []byte {
	b.Helper()

	payload, err := proto.Marshal(m)
	require.NoError(b, err)
	return payload
}

func BenchmarkExtractor_NamespacedRequest(b *testing.B) {
	payload := benchMarshal(b, testutils.StartWorkflowRequest())
	ex := protoutil.NewExtractor(protoregistry.GlobalFiles, protoregistry.GlobalTypes)
	require.Equal(b, "payments-production", ex.Namespace(startMethod, payload), "warm-up call must extract the namespace")

	b.ReportAllocs()
	for b.Loop() {
		if ns := ex.Namespace(startMethod, payload); ns == "" {
			b.Fatal("namespace was not extracted")
		}
	}
}

// BenchmarkExtractor_SystemRequest covers the request the SDK sends on every
// connect: no namespace accessor, so the full unmarshal runs and the cast
// misses.
func BenchmarkExtractor_SystemRequest(b *testing.B) {
	payload := benchMarshal(b, &workflowservice.GetSystemInfoRequest{})
	ex := protoutil.NewExtractor(protoregistry.GlobalFiles, protoregistry.GlobalTypes)
	require.Equal(b, "", ex.Namespace(systemMethod, payload))

	b.ReportAllocs()
	for b.Loop() {
		if ns := ex.Namespace(systemMethod, payload); ns != "" {
			b.Fatal("system request unexpectedly carried a namespace")
		}
	}
}
