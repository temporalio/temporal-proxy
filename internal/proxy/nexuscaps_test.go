package proxy_test

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	nexuspb "go.temporal.io/api/nexus/v1"
	"go.temporal.io/api/workflowservice/v1"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"

	"go.temporal.io/proxy/internal/proxy"
	"go.temporal.io/proxy/internal/transport/meta"
	"go.temporal.io/proxy/pkg/logger"
	"go.temporal.io/proxy/pkg/logger/tag"
)

const legacyNexusFamily = "proxy_encryption_nexus_tasks_without_failure_support_total"

// warnCounter is a test logger that also counts each warning by message and
// the value of its namespace tag.
type warnCounter struct {
	*logger.TestLogger

	mu     sync.Mutex
	counts map[string]int
}

func newWarnCounter() *warnCounter {
	return &warnCounter{TestLogger: logger.NewTestLogger(), counts: map[string]int{}}
}

func (w *warnCounter) Warn(msg string, tags ...tag.Tag) {
	w.TestLogger.Warn(msg, tags...)

	w.mu.Lock()
	defer w.mu.Unlock()

	for _, t := range tags {
		if t.Key == "namespace" {
			w.counts[msg+"/"+fmt.Sprint(t.Value)]++
		}
	}
}

// warned is how many times msg was warned with the namespace tag ns.
func (w *warnCounter) warned(msg, ns string) int {
	w.mu.Lock()
	defer w.mu.Unlock()

	return w.counts[msg+"/"+ns]
}

// pollNexusTask returns a polled Nexus task, carrying the
// temporal_failure_responses capability as given, or an empty long-poll
// response when token is empty.
func pollNexusTask(token string, failureSupport bool) *workflowservice.PollNexusTaskQueueResponse {
	if token == "" {
		return &workflowservice.PollNexusTaskQueueResponse{}
	}

	return &workflowservice.PollNexusTaskQueueResponse{
		TaskToken: []byte(token),
		Request: &nexuspb.Request{
			Capabilities: &nexuspb.Request_Capabilities{TemporalFailureResponses: failureSupport},
		},
	}
}

// pollThrough sends one poll for namespace through interceptor, answered with
// polled.
func pollThrough(
	t *testing.T, interceptor grpc.UnaryClientInterceptor, namespace string, polled *workflowservice.PollNexusTaskQueueResponse,
) {
	t.Helper()

	invoker := func(_ context.Context, _ string, _, got any, _ *grpc.ClientConn, _ ...grpc.CallOption) error {
		proto.Merge(got.(proto.Message), polled)
		return nil
	}

	reply := &workflowservice.PollNexusTaskQueueResponse{}
	err := interceptor(meta.WithNamespace(t.Context(), namespace), "/temporal.api.workflowservice.v1.WorkflowService/PollNexusTaskQueue",
		&workflowservice.PollNexusTaskQueueRequest{Namespace: namespace}, reply, nil, invoker)
	require.NoError(t, err)
}

// TestCodecInterceptorCountsNexusTasksWithoutFailureSupport pins that, with
// encryption on, every Nexus task polled without temporal_failure_responses is
// counted by namespace and warned about once per namespace, since its Worker
// would return a failure in a shape no codec sees. A task with the capability,
// or a long poll that returned no task, is not counted.
func TestCodecInterceptorCountsNexusTasksWithoutFailureSupport(t *testing.T) {
	t.Parallel()

	reg := prometheus.NewRegistry()
	log := newWarnCounter()
	interceptor, err := proxy.CodecInterceptor(proxy.CodecOptions{
		DecodeLimiter: testLimiter(),
		ResponseLimit: testResponseLimit,
		Vault:         &fakeVault{},
		Encrypt:       true,
		Reporter:      bindingReporter(reg),
		Logger:        log,
	})
	require.NoError(t, err)

	pollThrough(t, interceptor, "payments", pollNexusTask("t1", false))
	pollThrough(t, interceptor, "payments", pollNexusTask("t2", false))
	pollThrough(t, interceptor, "payments", pollNexusTask("t3", true))
	pollThrough(t, interceptor, "payments", pollNexusTask("", false))
	pollThrough(t, interceptor, "orders", pollNexusTask("t4", false))

	family := gatherFamily(t, reg, legacyNexusFamily)
	require.NotNil(t, family)

	counts := map[string]float64{}
	for _, m := range family.GetMetric() {
		for _, l := range m.GetLabel() {
			if l.GetName() == "namespace" {
				counts[l.GetValue()] = m.GetCounter().GetValue()
			}
		}
	}
	require.Equal(t, map[string]float64{"payments": 2, "orders": 1}, counts)

	require.Equal(t, 1, log.warned(proxy.LegacyNexusFailureWarning, "payments"), "warned once per namespace")
	require.Equal(t, 1, log.warned(proxy.LegacyNexusFailureWarning, "orders"))
}

// TestCodecInterceptorIgnoresNexusCapabilitiesWithoutEncryption pins that,
// with nothing sealed, a Nexus task without the capability is neither counted
// nor warned about, since there is nothing for its failure to bypass.
func TestCodecInterceptorIgnoresNexusCapabilitiesWithoutEncryption(t *testing.T) {
	t.Parallel()

	reg := prometheus.NewRegistry()
	log := newWarnCounter()
	interceptor, err := proxy.CodecInterceptor(proxy.CodecOptions{
		DecodeLimiter: testLimiter(),
		ResponseLimit: testResponseLimit,
		Vault:         &fakeVault{},
		Reporter:      bindingReporter(reg),
		Logger:        log,
	})
	require.NoError(t, err)

	pollThrough(t, interceptor, "payments", pollNexusTask("t1", false))

	family := gatherFamily(t, reg, legacyNexusFamily)
	require.Zero(t, len(family.GetMetric()))
	require.Zero(t, log.warned(proxy.LegacyNexusFailureWarning, "payments"))
	require.False(t, log.Contains(proxy.LegacyNexusFailureWarning))
}
