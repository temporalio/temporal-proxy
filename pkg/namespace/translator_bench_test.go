package namespace_test

import (
	"testing"

	failurepb "go.temporal.io/api/failure/v1"
	namespacepb "go.temporal.io/api/namespace/v1"

	"github.com/temporalio/temporal-proxy/pkg/namespace"
)

func BenchmarkTranslator_ToLocal_NamespaceInfo(b *testing.B) {
	tr := namespace.New(nil)
	msg := &namespacepb.NamespaceInfo{Name: "payments"}

	b.ReportAllocs()
	for b.Loop() {
		tr.ToLocal(msg)
	}
}

// BenchmarkTranslator_ToLocal_DeepRecursion measures the cost of recursive
// descent. Failure.Cause is a self-recursive proto field, so a chain of N
// Failures forces the walker N levels deep through one message at each step.
func BenchmarkTranslator_ToLocal_DeepRecursion(b *testing.B) {
	tr := namespace.New(nil)
	msg := &failurepb.Failure{Message: "leaf"}
	for range 100 {
		msg = &failurepb.Failure{Message: "level", Cause: msg}
	}

	b.ReportAllocs()
	for b.Loop() {
		tr.ToLocal(msg)
	}
}
