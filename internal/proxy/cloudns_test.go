package proxy_test

import (
	"context"
	"net"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
	workflowservice "go.temporal.io/api/workflowservice/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/temporalio/temporal-proxy/internal/cloud"
	"github.com/temporalio/temporal-proxy/internal/proxy"
	"github.com/temporalio/temporal-proxy/internal/transport/meta"
	"github.com/temporalio/temporal-proxy/pkg/logger"
	"github.com/temporalio/temporal-proxy/pkg/logger/tag"
)

// invalidNSMsg is the entry the cloud namespace check emits.
const invalidNSMsg = "outbound namespace is not valid"

// countingLogger counts Debug entries, which TestLogger does not expose.
type countingLogger struct {
	logger.Logger

	debugs *atomic.Int64
}

func TestCloudNamespaceDialOptionsUnary(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		ctx      func(*testing.T) context.Context
		out      func(string) string
		nilLog   bool
		wantTags []tag.Tag
	}{
		{
			name: "cloud-shaped translation is quiet",
			ctx:  namespacedContext("orders"),
			out:  cloudRemote,
		},
		{
			name: "not cloud-shaped is reported",
			ctx:  namespacedContext("orders"),
			out:  badRemote,
			wantTags: []tag.Tag{
				tag.String("upstream", "cloud"),
				tag.Component("translation"),
				tag.String("method", "/svc/Method"),
				tag.String("localNamespace", "orders"),
				tag.String("remoteNamespace", "orders."),
				tag.Error(cloud.ValidateNamespace("orders.")),
			},
		},
		{
			// A Cloud upstream with no translation rules expects clients to send
			// fully-qualified names, so a short one is wrong on its own.
			name: "identity translation still checks the local name",
			ctx:  namespacedContext("orders"),
			out:  func(s string) string { return s },
			wantTags: []tag.Tag{
				tag.String("upstream", "cloud"),
				tag.Component("translation"),
				tag.String("method", "/svc/Method"),
				tag.String("localNamespace", "orders"),
				tag.String("remoteNamespace", "orders"),
				tag.Error(cloud.ValidateNamespace("orders")),
			},
		},
		{
			name: "no namespace on the context",
			ctx:  func(t *testing.T) context.Context { return t.Context() },
			out:  badRemote,
		},
		{
			// The router stamps the header even for namespace-less calls such as
			// GetSystemInfo, so an empty value must not be translated.
			name: "empty namespace value",
			ctx:  namespacedContext(""),
			out:  badRemote,
		},
		{
			name:   "nil logger",
			ctx:    namespacedContext("orders"),
			out:    badRemote,
			nilLog: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			log := logger.NewTestLogger()

			var passed logger.Logger
			if !tt.nilLog {
				passed = log.With(tag.String("upstream", "cloud"))
			}

			cc := clientWithOptions(t, "passthrough:///127.0.0.1:1", proxy.CloudNamespaceDialOptions(tt.out, passed))

			ctx, cancel := context.WithCancel(tt.ctx(t))
			cancel()

			err := cc.Invoke(
				ctx,
				"/svc/Method",
				&workflowservice.StartWorkflowExecutionRequest{},
				&workflowservice.StartWorkflowExecutionResponse{},
			)
			require.Error(t, err, "the canceled call still fails; the check does not swallow it")

			if len(tt.wantTags) == 0 {
				require.False(t, log.Contains(invalidNSMsg))
				return
			}

			require.True(t, log.ContainsEntry(logger.LevelDebug, invalidNSMsg, tt.wantTags...))
		})
	}
}

func TestCloudNamespaceDialOptionsStreamChecksOncePerOpen(t *testing.T) {
	t.Parallel()

	log := logger.NewTestLogger()
	debugs := new(atomic.Int64)
	counting := countingLogger{Logger: log.With(tag.String("upstream", "cloud")), debugs: debugs}

	// A live stream is the only way to send more than once, so this needs a real
	// server. It only drains, which is enough to keep SendMsg deterministic.
	cc := clientWithOptions(t, drainingServer(t), proxy.CloudNamespaceDialOptions(badRemote, counting))

	cs, err := cc.NewStream(
		meta.WithNamespace(t.Context(), "orders"),
		&grpc.StreamDesc{ClientStreams: true, ServerStreams: true},
		"/svc/Stream",
	)
	require.NoError(t, err)

	for range 3 {
		require.NoError(t, cs.SendMsg(&workflowservice.StartWorkflowExecutionRequest{Namespace: "orders"}))
	}

	require.Equal(t, int64(1), debugs.Load(), "opening the stream checks once; messages do not")
	require.True(t, log.ContainsEntry(
		logger.LevelDebug, invalidNSMsg,
		tag.String("upstream", "cloud"),
		tag.Component("translation"),
		tag.String("method", "/svc/Stream"),
		tag.String("localNamespace", "orders"),
		tag.String("remoteNamespace", "orders."),
		tag.Error(cloud.ValidateNamespace("orders.")),
	))
}

func (c countingLogger) Debug(msg string, tags ...tag.Tag) {
	c.debugs.Add(1)
	c.Logger.Debug(msg, tags...)
}

// With keeps the counter attached to derived loggers, which matters because
// CloudNamespaceDialOptions decorates the logger it is handed.
func (c countingLogger) With(tags ...tag.Tag) logger.Logger {
	return countingLogger{Logger: c.Logger.With(tags...), debugs: c.debugs}
}

// namespacedContext builds a context carrying ns the way the router stamps it.
func namespacedContext(ns string) func(*testing.T) context.Context {
	return func(t *testing.T) context.Context {
		return meta.WithNamespace(t.Context(), ns)
	}
}

// clientWithOptions returns a lazy client to target with opts installed. It
// dials nothing until a call is made.
func clientWithOptions(t *testing.T, target string, opts []grpc.DialOption) *grpc.ClientConn {
	t.Helper()

	cc, err := grpc.NewClient(
		target,
		append([]grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}, opts...)...,
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = cc.Close() })

	return cc
}

// drainingServer starts a gRPC server that accepts any method and reads every
// message until the client stops sending, and returns its address. It exists so
// a test can send on a real stream without a service implementation.
func drainingServer(t *testing.T) string {
	t.Helper()

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	srv := grpc.NewServer(grpc.UnknownServiceHandler(func(_ any, stream grpc.ServerStream) error {
		for {
			if err := stream.RecvMsg(new(workflowservice.StartWorkflowExecutionRequest)); err != nil {
				return nil
			}
		}
	}))

	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)

	return lis.Addr().String()
}

// cloudRemote maps a local name to a well-formed Cloud namespace.
func cloudRemote(s string) string { return s + ".a1b2c" }

// badRemote maps a local name the way an unset account variable would, leaving a
// trailing dot and no account id.
func badRemote(s string) string { return s + "." }
