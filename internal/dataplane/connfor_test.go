package dataplane_test

import (
	"bytes"
	"encoding/base64"
	"net/url"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/api/common/v1"
	"go.temporal.io/api/operatorservice/v1"
	"go.temporal.io/api/query/v1"
	"go.temporal.io/api/workflowservice/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"go.temporal.io/proxy/internal/config"
	"go.temporal.io/proxy/internal/dataplane/dataplanetest"
	"go.temporal.io/proxy/internal/services"
	"go.temporal.io/proxy/internal/transport/meta"
)

// upstreamHeaders are the metadata keys the per-upstream proxy derives for the
// upstream, so a call through ConnFor must deliver the same values as one
// through the gateway.
var upstreamHeaders = []string{"authorization", meta.TemporalNamespaceHeader, meta.NamespaceHeader}

// startNexusOperation is a StartNexusOperationExecution request in ns carrying
// input.
func startNexusOperation(ns string, input *common.Payload) *workflowservice.StartNexusOperationExecutionRequest {
	return &workflowservice.StartNexusOperationExecutionRequest{
		Namespace:   ns,
		OperationId: "op-1",
		Endpoint:    "payments-ep",
		Service:     "payments",
		Operation:   "charge",
		Input:       input,
	}
}

func TestConnForRoutesByNamespaceLikeTheGateway(t *testing.T) {
	t.Parallel()

	orders := dataplanetest.NewTLSUpstream(t)
	workers := dataplanetest.NewUpstream(t)

	cfg := dataplanetest.Config(workers)
	cfg.Upstreams = append(cfg.Upstreams, config.Upstream{
		Name:        "orders",
		Listen:      orders.Listen(),
		Namespaces:  config.NamespaceConfig{Rules: config.NamespaceRules{Suffix: ".remote"}},
		Credentials: &config.CredentialConfig{Static: &config.StaticCredentialConfig{APIKey: "k3y"}},
	})
	cfg.Routing.Rules = []config.RoutingRule{{Upstream: "orders", Match: config.RoutingMatch{Namespace: "orders"}}}

	f := dataplanetest.Start(t, cfg)

	conn, err := f.Dataplane().ConnFor("orders")
	require.NoError(t, err)

	// The SDK stamps temporal-namespace on every call, so the gateway call does
	// too. ConnFor's caller is not an SDK and need not.
	gatewayCtx := metadata.AppendToOutgoingContext(f.Context(), meta.TemporalNamespaceHeader, "orders")
	_, err = f.Client().StartNexusOperationExecution(gatewayCtx, startNexusOperation("orders", nil), grpc.WaitForReady(true))
	require.NoError(t, err)
	viaGateway := orders.Metadata()

	_, err = workflowservice.NewWorkflowServiceClient(conn).StartNexusOperationExecution(
		f.Context(), startNexusOperation("orders", nil), grpc.WaitForReady(true),
	)
	require.NoError(t, err)
	viaConn := orders.Metadata()

	reqs := orders.Requests()
	require.Len(t, reqs, 2)
	require.Equal(t, "orders.remote", reqs[1].(*workflowservice.StartNexusOperationExecutionRequest).GetNamespace())
	require.True(t, proto.Equal(reqs[0], reqs[1]), "the upstream must receive the request the gateway would send")

	require.Equal(t, []string{"Bearer k3y"}, viaConn.Get("authorization"))
	require.Equal(t, []string{"orders"}, viaConn.Get(meta.NamespaceHeader))
	for _, key := range upstreamHeaders {
		require.Equal(t, viaGateway.Get(key), viaConn.Get(key), key)
	}

	require.Empty(t, workers.Requests(), "the default upstream must not see a request a rule routed")
}

func TestConnForOverridesCallerSuppliedNamespaceHeaders(t *testing.T) {
	t.Parallel()

	up := dataplanetest.NewUpstream(t)

	cfg := dataplanetest.Config(up)
	cfg.Upstreams[0].Namespaces = config.NamespaceConfig{Rules: config.NamespaceRules{Suffix: ".remote"}}

	f := dataplanetest.Start(t, cfg)

	conn, err := f.Dataplane().ConnFor("orders")
	require.NoError(t, err)

	ctx := metadata.AppendToOutgoingContext(
		f.Context(),
		meta.NamespaceHeader, "forged",
		meta.TemporalNamespaceHeader, "payments",
	)
	_, err = workflowservice.NewWorkflowServiceClient(conn).StartNexusOperationExecution(
		ctx, startNexusOperation("orders", nil), grpc.WaitForReady(true),
	)
	require.NoError(t, err)

	// Neither header the caller sent survives: the upstream sees the bound
	// namespace, translated where the per-upstream proxy translates it.
	require.Equal(t, []string{"orders"}, up.Metadata().Get(meta.NamespaceHeader))
	require.Equal(t, []string{"orders.remote"}, up.Metadata().Get(meta.TemporalNamespaceHeader))
}

func TestConnForRejectsARequestWithNoNamespace(t *testing.T) {
	t.Parallel()

	up := dataplanetest.NewUpstream(t)
	f := dataplanetest.Start(t, dataplanetest.Config(up))

	conn, err := f.Dataplane().ConnFor("orders")
	require.NoError(t, err)

	_, err = workflowservice.NewWorkflowServiceClient(conn).GetSystemInfo(
		f.Context(), &workflowservice.GetSystemInfoRequest{}, grpc.WaitForReady(true),
	)
	require.Equal(t, codes.InvalidArgument, status.Code(err), "a request type with no namespace field: %v", err)
	require.Empty(t, up.Requests())
}

func TestConnForRefusesAServiceOutsideTheAllowlist(t *testing.T) {
	t.Parallel()

	up := dataplanetest.NewUpstream(t)

	cfg := dataplanetest.Config(up)
	cfg.AllowedServices = config.Services{services.WorkflowService}

	f := dataplanetest.Start(t, cfg)

	conn, err := f.Dataplane().ConnFor("orders")
	require.NoError(t, err)

	_, err = operatorservice.NewOperatorServiceClient(conn).ListSearchAttributes(
		f.Context(), &operatorservice.ListSearchAttributesRequest{Namespace: "orders"}, grpc.WaitForReady(true),
	)
	require.Equal(t, codes.Unimplemented, status.Code(err), "a service the gateway would refuse: %v", err)

	stream, err := conn.NewStream(
		f.Context(), &grpc.StreamDesc{ClientStreams: true}, "/"+services.OperatorService+"/ListSearchAttributes",
	)
	require.Nil(t, stream)
	require.Equal(t, codes.Unimplemented, status.Code(err), "a stream for a refused service: %v", err)

	require.Empty(t, up.Requests())
}

func TestConnForIgnoresMetadataRules(t *testing.T) {
	t.Parallel()

	cell := dataplanetest.NewUpstream(t)
	workers := dataplanetest.NewUpstream(t)

	cfg := dataplanetest.Config(workers)
	cfg.Upstreams = append(cfg.Upstreams, config.Upstream{Name: "cell", Listen: cell.Listen()})
	cfg.Routing.Rules = []config.RoutingRule{{
		Upstream: "cell",
		Match:    config.RoutingMatch{Metadata: map[string]string{"x-cell": "*"}},
	}}

	f := dataplanetest.Start(t, cfg)

	conn, err := f.Dataplane().ConnFor("orders")
	require.NoError(t, err)

	// Metadata on the call does not reroute it: the upstream was chosen without
	// any.
	ctx := metadata.AppendToOutgoingContext(f.Context(), "x-cell", "a")
	_, err = workflowservice.NewWorkflowServiceClient(conn).StartNexusOperationExecution(
		ctx, startNexusOperation("orders", nil), grpc.WaitForReady(true),
	)
	require.NoError(t, err)

	require.Len(t, workers.Requests(), 1)
	require.Empty(t, cell.Requests())
}

func TestConnForFailsWhenNothingRoutes(t *testing.T) {
	t.Parallel()

	up := dataplanetest.NewUpstream(t)

	cfg := dataplanetest.Config(up)
	cfg.Routing = config.Routing{Rules: []config.RoutingRule{{
		Upstream: dataplanetest.DefaultUpstream,
		Match:    config.RoutingMatch{Namespace: "orders"},
	}}}

	f := dataplanetest.Start(t, cfg)

	_, err := f.Dataplane().ConnFor("payments")
	require.ErrorContains(t, err, `no upstream routes namespace "payments"`)

	_, err = f.Dataplane().ConnFor("")
	require.ErrorContains(t, err, "a namespace is required", "a connection for no namespace has nothing to route on")
}

func TestConnForRejectsARequestForAnotherNamespace(t *testing.T) {
	t.Parallel()

	up := dataplanetest.NewUpstream(t)
	f := dataplanetest.Start(t, dataplanetest.Config(up))

	conn, err := f.Dataplane().ConnFor("orders")
	require.NoError(t, err)

	client := workflowservice.NewWorkflowServiceClient(conn)
	for _, ns := range []string{"payments", ""} {
		_, err = client.StartNexusOperationExecution(f.Context(), startNexusOperation(ns, nil))
		require.Equal(t, codes.InvalidArgument, status.Code(err), "namespace %q", ns)
	}

	require.Empty(t, up.Requests())
}

func TestConnForSealsPayloadsLikeTheGateway(t *testing.T) {
	t.Parallel()

	up := dataplanetest.NewUpstream(t)

	key := base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{0x2a}, 32))
	uri, err := url.Parse("testing://" + key)
	require.NoError(t, err)

	cfg := dataplanetest.Config(up)
	cfg.Upstreams[0].Namespaces = config.NamespaceConfig{Rules: config.NamespaceRules{Suffix: ".remote"}}
	cfg.Encryption = config.Encryption{
		Enabled:          true,
		NamespaceBinding: config.NamespaceBindingStrict,
		Default:          &config.KeyPolicy{URI: *uri, Duration: time.Hour},
	}

	f := dataplanetest.StartApp(t, cfg)

	conn, err := f.Dataplane().ConnFor("orders")
	require.NoError(t, err)

	secret := &common.Payload{
		Metadata: map[string][]byte{"encoding": []byte("json/plain")},
		Data:     []byte(`"the-answer-is-42"`),
	}

	_, err = f.Client().StartNexusOperationExecution(f.Context(), startNexusOperation("orders", secret), grpc.WaitForReady(true))
	require.NoError(t, err)

	_, err = workflowservice.NewWorkflowServiceClient(conn).StartNexusOperationExecution(
		f.Context(), startNexusOperation("orders", secret), grpc.WaitForReady(true),
	)
	require.NoError(t, err)

	reqs := up.Requests()
	require.Len(t, reqs, 2)

	viaGateway := reqs[0].(*workflowservice.StartNexusOperationExecutionRequest).GetInput()
	viaConn := reqs[1].(*workflowservice.StartNexusOperationExecutionRequest).GetInput()

	require.Equal(t, "binary/encrypted", string(viaConn.GetMetadata()["encoding"]))
	require.NotEqual(t, secret.GetData(), viaConn.GetData())

	// Each seal draws its own data key, so the ciphertext differs; everything
	// that says how and for whom it was sealed must not.
	require.Equal(t, len(viaGateway.GetMetadata()), len(viaConn.GetMetadata()))
	for k, v := range viaGateway.GetMetadata() {
		if k == "encryption-dek" {
			continue
		}

		require.Equal(t, string(v), string(viaConn.GetMetadata()[k]), k)
	}

	// The response direction opens payloads too: the fake upstream echoes the
	// sealed query arguments back, and the caller reads them in the clear.
	resp, err := workflowservice.NewWorkflowServiceClient(conn).QueryWorkflow(
		f.Context(),
		&workflowservice.QueryWorkflowRequest{
			Namespace: "orders",
			Execution: &common.WorkflowExecution{WorkflowId: "wf-1"},
			Query: &query.WorkflowQuery{
				QueryType: "state",
				QueryArgs: &common.Payloads{Payloads: []*common.Payload{secret}},
			},
		},
	)
	require.NoError(t, err)
	require.Len(t, resp.GetQueryResult().GetPayloads(), 1)
	require.True(t, proto.Equal(secret, resp.GetQueryResult().GetPayloads()[0]))
}

func TestConnForResolvesATemplatedUpstreamPerNamespace(t *testing.T) {
	t.Parallel()

	orders := dataplanetest.NewUpstream(t)
	others := dataplanetest.NewUpstream(t)

	f := dataplanetest.Start(t, &config.Config{
		Routing: config.Routing{DefaultUpstream: "dynamic"},
		Upstreams: config.UpstreamList{{
			Name: "dynamic",
			Listen: config.ListenConfig{
				HostPort: `{{ if eq .LocalNamespace "orders" }}` + orders.Addr() + `{{ else }}` + others.Addr() + `{{ end }}`,
				Insecure: true,
			},
		}},
	})

	for _, ns := range []string{"orders", "payments"} {
		conn, err := f.Dataplane().ConnFor(ns)
		require.NoError(t, err)

		_, err = workflowservice.NewWorkflowServiceClient(conn).StartNexusOperationExecution(
			f.Context(), startNexusOperation(ns, nil), grpc.WaitForReady(true),
		)
		require.NoError(t, err)
	}

	require.Len(t, orders.Requests(), 1)
	require.Equal(t, "orders", orders.Requests()[0].(*workflowservice.StartNexusOperationExecutionRequest).GetNamespace())
	require.Len(t, others.Requests(), 1)
	require.Equal(t, "payments", others.Requests()[0].(*workflowservice.StartNexusOperationExecutionRequest).GetNamespace())
}
