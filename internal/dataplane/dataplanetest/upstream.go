package dataplanetest

import (
	"context"
	"crypto/tls"
	"net"
	"slices"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/api/workflowservice/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"

	"github.com/temporalio/temporal-proxy/internal/config"
	"github.com/temporalio/temporal-proxy/pkg/testutil"
)

// Upstream is a fake Temporal frontend standing in for the service a
// dataplane forwards to. It records every request and its metadata, answers
// GetSystemInfo with an empty response, and echoes QueryWorkflow's arguments
// back as its result.
type Upstream struct {
	workflowservice.UnimplementedWorkflowServiceServer

	addr string
	tls  *config.TLSConfig

	// mu guards the recorded state, which the serving goroutine writes while
	// the test reads.
	mu       sync.Mutex
	metadata metadata.MD
	requests []proto.Message
}

// NewUpstream starts a fake frontend on a loopback port over plaintext and
// stops it when the test ends.
func NewUpstream(t *testing.T) *Upstream {
	t.Helper()

	return newUpstream(t, nil)
}

// NewTLSUpstream starts a fake frontend over TLS. Its [Upstream.TLSConfig]
// carries the CA and client identity needed to dial it, which is the only way
// to exercise credentials that refuse to travel over an insecure transport.
func NewTLSUpstream(t *testing.T) *Upstream {
	t.Helper()

	caFile, serverCertFile, serverKeyFile := testutil.GenerateMTLSCerts(t)
	serverCert, err := tls.LoadX509KeyPair(serverCertFile, serverKeyFile)
	require.NoError(t, err)

	clientCertFile, clientKeyFile := testutil.GenerateRSACert(t)

	up := newUpstream(t, credentials.NewTLS(&tls.Config{
		Certificates: []tls.Certificate{serverCert},
		MinVersion:   tls.VersionTLS12,
	}))

	up.tls = &config.TLSConfig{
		CA:   caFile,
		Cert: clientCertFile,
		Key:  clientKeyFile,
		// The leaf certificate advertises CN/DNSNames "localhost", which does
		// not match the 127.0.0.1 dial address.
		ServerName: "localhost",
	}

	return up
}

// Addr is the host:port the fake frontend is accepting on.
func (u *Upstream) Addr() string { return u.addr }

// GetSystemInfo records the call and answers with an empty response. It is the
// smallest WorkflowService method that needs no namespace argument.
func (u *Upstream) GetSystemInfo(
	ctx context.Context, req *workflowservice.GetSystemInfoRequest,
) (*workflowservice.GetSystemInfoResponse, error) {
	u.record(ctx, req)

	return &workflowservice.GetSystemInfoResponse{}, nil
}

// Metadata is the incoming metadata of the most recent request, or nil before
// the first one arrives.
func (u *Upstream) Metadata() metadata.MD {
	u.mu.Lock()
	defer u.mu.Unlock()

	// Copy turns a nil MD into an empty one, which would make "did a request
	// arrive" checks pass before any did.
	if u.metadata == nil {
		return nil
	}

	return u.metadata.Copy()
}

// QueryWorkflow records the call and echoes the query arguments back as the
// result, so one call exercises both directions of an interceptor that rewrites
// payloads.
func (u *Upstream) QueryWorkflow(
	ctx context.Context, req *workflowservice.QueryWorkflowRequest,
) (*workflowservice.QueryWorkflowResponse, error) {
	u.record(ctx, req)

	return &workflowservice.QueryWorkflowResponse{QueryResult: req.GetQuery().GetQueryArgs()}, nil
}

// Requests returns every request received so far, in arrival order.
func (u *Upstream) Requests() []proto.Message {
	u.mu.Lock()
	defer u.mu.Unlock()

	return slices.Clone(u.requests)
}

// TLSConfig is the client-side configuration needed to dial this upstream, or
// nil when it serves plaintext.
func (u *Upstream) TLSConfig() *config.TLSConfig { return u.tls }

func (u *Upstream) record(ctx context.Context, req proto.Message) {
	md, _ := metadata.FromIncomingContext(ctx)

	u.mu.Lock()
	defer u.mu.Unlock()

	u.metadata = md
	u.requests = append(u.requests, req)
}

// newUpstream serves a fake frontend on an ephemeral loopback port. The
// ephemeral port also keeps the socket path the proxy derives from it unique
// across parallel tests.
func newUpstream(t *testing.T, creds credentials.TransportCredentials) *Upstream {
	t.Helper()

	up := new(Upstream)

	var opts []grpc.ServerOption
	if creds != nil {
		opts = append(opts, grpc.Creds(creds))
	}

	svr := grpc.NewServer(opts...)
	workflowservice.RegisterWorkflowServiceServer(svr, up)

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	go func() { _ = svr.Serve(lis) }()
	t.Cleanup(svr.Stop)

	up.addr = lis.Addr().String()

	return up
}
