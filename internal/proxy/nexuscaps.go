package proxy

import (
	"context"
	"sync"

	"go.temporal.io/api/workflowservice/v1"
	"google.golang.org/grpc"

	"go.temporal.io/proxy/internal/transport/meta"
	"go.temporal.io/proxy/pkg/logger"
	"go.temporal.io/proxy/pkg/logger/tag"
)

// LegacyNexusFailureWarning is logged, once per namespace, when a Worker polls
// a Nexus task without the temporal_failure_responses capability.
const LegacyNexusFailureWarning = "Polled a Nexus task without temporal_failure_responses; " +
	"its handler failures are returned in a shape no codec can seal, so they reach the upstream unencrypted"

// nexusFailureWatch notices Nexus tasks polled without the
// temporal_failure_responses capability. A task gets it from the caller's
// temporal-nexus-failure-support header, which Temporal Server 1.31 and later
// send for Workflow and standalone Nexus operation callers. Without it, the Go
// SDK returns a handler's failure as a legacy nexus.Failure, whose message is a
// plain string and whose cause chain and details are protojson bytes the payload
// and failure visitors never look inside, so encryption cannot reach them. The
// proxy does not change the task; it counts each one and warns once per
// namespace, so an exposure is visible.
type nexusFailureWatch struct {
	reporter *Reporter
	log      logger.Logger
	warned   sync.Map // namespace -> struct{}
}

// intercept counts the Nexus task in reply when it lacks the capability. A poll
// that returned no task, an error, or any other reply is left alone.
func (w *nexusFailureWatch) intercept(
	ctx context.Context,
	method string,
	req, reply any,
	cc *grpc.ClientConn,
	invoker grpc.UnaryInvoker,
	opts ...grpc.CallOption,
) error {
	if err := invoker(ctx, method, req, reply, cc, opts...); err != nil {
		return err
	}

	polled, ok := reply.(*workflowservice.PollNexusTaskQueueResponse)
	if !ok || len(polled.GetTaskToken()) == 0 {
		return nil
	}

	if polled.GetRequest().GetCapabilities().GetTemporalFailureResponses() {
		return nil
	}

	ns := meta.NamespaceFrom(ctx)
	w.reporter.NexusTaskWithoutFailureSupport(ctx, ns)

	if w.log == nil {
		return nil
	}

	if _, seen := w.warned.LoadOrStore(ns, struct{}{}); !seen {
		w.log.Warn(LegacyNexusFailureWarning, tag.String("namespace", ns))
	}

	return nil
}
