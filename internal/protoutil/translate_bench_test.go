package protoutil_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/api/namespace/v1"
	"go.temporal.io/api/workflowservice/v1"
	"google.golang.org/protobuf/reflect/protoregistry"

	"github.com/temporalio/temporal-proxy/internal/protoutil"
	"github.com/temporalio/temporal-proxy/internal/testutils"
)

func BenchmarkTranslator_StartWorkflowRequest(b *testing.B) {
	tr := protoutil.NewTranslator(protoregistry.GlobalFiles)
	require.NoError(b, tr.WarmService(wfService))
	req := testutils.StartWorkflowRequest()

	b.ReportAllocs()
	for b.Loop() {
		// The mapper returns a fixed name so repeated in-place rewrites stay
		// stable and the benchmark measures the walk, not string growth.
		tr.Translate(req, func(string) string { return "payments-production-remote" })
	}
}

func BenchmarkTranslator_DescribeNamespaceResponse(b *testing.B) {
	tr := protoutil.NewTranslator(protoregistry.GlobalFiles)
	require.NoError(b, tr.WarmService(wfService))
	resp := &workflowservice.DescribeNamespaceResponse{
		NamespaceInfo: &namespace.NamespaceInfo{Name: "payments-production"},
	}

	b.ReportAllocs()
	for b.Loop() {
		tr.Translate(resp, func(string) string { return "payments-production-local" })
	}
}
