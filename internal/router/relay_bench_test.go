package router_test

import (
	"context"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/stretchr/testify/require"
	"go.temporal.io/api/workflowservice/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/reflect/protoregistry"

	"github.com/temporalio/temporal-proxy/internal/config"
	"github.com/temporalio/temporal-proxy/internal/metrics"
	"github.com/temporalio/temporal-proxy/internal/protoutil"
	"github.com/temporalio/temporal-proxy/internal/proxy"
	"github.com/temporalio/temporal-proxy/internal/router"
	"github.com/temporalio/temporal-proxy/internal/server"
	"github.com/temporalio/temporal-proxy/internal/services"
	"github.com/temporalio/temporal-proxy/internal/testutils"
)

const (
	benchService = "temporal.api.workflowservice.v1.WorkflowService"
	benchMethod  = "/" + benchService + "/StartWorkflowExecution"
	benchRunID   = "0198f4a2-c0de-7f31-a5b2-9e11d1c2a3b4"
)

// benchUpstreamServer returns a gRPC server that answers StartWorkflowExecution
// under the real service name, so the forwarder resolves the method against the
// production descriptor and type registries. The handler decodes the request and
// returns a run, which is the observable part of the response SDKs rely on.
func benchUpstreamServer() *grpc.Server {
	desc := grpc.ServiceDesc{
		ServiceName: benchService,
		HandlerType: (*any)(nil),
		Methods: []grpc.MethodDesc{{
			MethodName: "StartWorkflowExecution",
			Handler: func(_ any, _ context.Context, dec func(any) error, _ grpc.UnaryServerInterceptor) (any, error) {
				req := new(workflowservice.StartWorkflowExecutionRequest)
				if err := dec(req); err != nil {
					return nil, err
				}

				return &workflowservice.StartWorkflowExecutionResponse{RunId: benchRunID}, nil
			},
		}},
		Streams: []grpc.StreamDesc{},
	}

	srv := grpc.NewServer()
	srv.RegisterService(&desc, nil)
	return srv
}

// benchInvoke runs one unary call and checks the response survived the round
// trip, so a codec or forwarder regression fails the benchmark instead of
// feeding it garbage.
func benchInvoke(b *testing.B, conn *grpc.ClientConn, req *workflowservice.StartWorkflowExecutionRequest) {
	b.Helper()

	resp := new(workflowservice.StartWorkflowExecutionResponse)
	if err := conn.Invoke(b.Context(), benchMethod, req, resp); err != nil {
		b.Fatal(err)
	}
	if got := resp.GetRunId(); got != benchRunID {
		b.Fatalf("response carried run id %q", got)
	}
}

// benchRelayConn stands up the production gateway shape: the hybrid codec, the
// server metrics interceptor, PeekInterceptor backed by the real extractor, and
// Handler backed by a real Director whose single upstream is a Forwarder over a
// bufconn connection to the fake upstream. Authentication (off by default) and
// panic recovery are not installed.
func benchRelayConn(b *testing.B) *grpc.ClientConn {
	b.Helper()

	upstreamLis := bufconn.Listen(1024 * 1024)
	upstream := benchUpstreamServer()
	serve(b, upstream, upstreamLis)
	upstreamConn := dialBufconn(b, upstreamLis)
	b.Cleanup(func() { _ = upstreamConn.Close() })

	fw, err := proxy.NewForwarder(upstreamConn)
	require.NoError(b, err)

	mux := router.MuxFor(prepared(b, config.Routing{DefaultUpstream: "primary"}))
	rep, _ := newTestReporter(b, "primary")
	d := router.NewDirector(mux, map[string]grpc.StreamHandler{"primary": fw.Handle}, rep, nil)

	extractor := protoutil.NewExtractor(protoregistry.GlobalFiles, protoregistry.GlobalTypes)
	allowlist := services.NewAllowlist(services.Default())

	reg := prometheus.NewRegistry()
	serverRep := server.NewReporter(
		metrics.New("tmprl_proxy", promauto.With(reg)).ForSubsystem("server"),
		metrics.MetadataLabels{},
	)

	relayLis := bufconn.Listen(1024 * 1024)
	relay := grpc.NewServer(
		grpc.ForceServerCodecV2(router.Codec()),
		grpc.ChainStreamInterceptor(
			serverRep.StreamInterceptor(),
			router.PeekInterceptor(extractor, allowlist),
		),
		grpc.UnknownServiceHandler(router.Handler(d, allowlist)),
	)
	serve(b, relay, relayLis)

	conn := dialBufconn(b, relayLis)
	b.Cleanup(func() { _ = conn.Close() })
	return conn
}

// BenchmarkUnaryThroughProxy measures one StartWorkflowExecution round trip as
// an SDK client observes it: client -> gateway (codec, metrics, peek, routing)
// -> forwarder (typed codec hop) -> upstream and back. Compare with
// BenchmarkUnaryDirect: the difference is what the proxy adds per unary call.
func BenchmarkUnaryThroughProxy(b *testing.B) {
	conn := benchRelayConn(b)
	req := testutils.StartWorkflowRequest()
	benchInvoke(b, conn, req)

	b.ReportAllocs()
	for b.Loop() {
		benchInvoke(b, conn, req)
	}
}

// BenchmarkUnaryDirect is the no-proxy baseline for BenchmarkUnaryThroughProxy:
// the same request to the same fake upstream over bufconn, with the standard
// proto codec, as an SDK client would speak to a Temporal Service directly.
func BenchmarkUnaryDirect(b *testing.B) {
	upstreamLis := bufconn.Listen(1024 * 1024)
	upstream := benchUpstreamServer()
	serve(b, upstream, upstreamLis)

	conn := dialBufconn(b, upstreamLis)
	b.Cleanup(func() { _ = conn.Close() })

	req := testutils.StartWorkflowRequest()
	benchInvoke(b, conn, req)

	b.ReportAllocs()
	for b.Loop() {
		benchInvoke(b, conn, req)
	}
}
