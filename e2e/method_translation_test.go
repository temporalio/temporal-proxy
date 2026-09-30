package e2e

import (
	"context"
	"net"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	operatorservice "go.temporal.io/api/operatorservice/v1"
	"go.temporal.io/api/workflowservice/v1"
	cloudservice "go.temporal.io/cloud-sdk/api/cloudservice/v1"
	cloudnamespace "go.temporal.io/cloud-sdk/api/namespace/v1"
	"go.temporal.io/cloud-sdk/api/resource/v1"
	"go.temporal.io/cloud-sdk/cloudclient"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	"github.com/temporalio/temporal-proxy/internal/config"
	"github.com/temporalio/temporal-proxy/internal/dataplane/dataplanetest"
)

// cloudUpstream is a fake Temporal Cloud control plane. It serves CloudService
// and nothing else, the way saas-api does, and records the request and metadata
// it was called with. It is local to this test rather than folded into
// dataplanetest.Upstream because WorkflowService and CloudService both declare
// UpdateNamespace, so one struct cannot embed both unimplemented servers.
type cloudUpstream struct {
	cloudservice.UnimplementedCloudServiceServer

	addr string

	mu    sync.Mutex
	got   *cloudservice.GetNamespacesRequest
	md    metadata.MD
	page  []*cloudnamespace.Namespace
	getNS string
}

// TestEndToEndListNamespacesTranslatesOntoCloudService drives the full stack the
// way the listnamespace example does: a client calls WorkflowService.ListNamespaces
// against the gateway, it is routed to the ordinary frontend like everything
// else, and the translation captures it there and answers it over the Cloud API
// connection instead. The client gets a ListNamespacesResponse back without ever
// knowing a different service on a different host answered it.
//
// This is the whole path - gateway, peek, router, per-upstream proxy, forwarder,
// translation interceptor - rather than the interceptor in isolation.
func TestEndToEndListNamespacesTranslatesOntoCloudService(t *testing.T) {
	t.Parallel()

	frontend := dataplanetest.NewUpstream(t)
	cloud := newCloudUpstream(t)
	cloud.setPage(&cloudnamespace.Namespace{
		Namespace: "payments.a1b2c",
		State:     resource.ResourceState_RESOURCE_STATE_ACTIVE,
		Spec: &cloudnamespace.NamespaceSpec{
			Name:          "payments",
			RetentionDays: 30,
			Replicas: []*cloudnamespace.ReplicaSpec{
				{Region: "aws-us-east-1"},
				{Region: "aws-us-west-2"},
			},
		},
	})

	f := dataplanetest.StartApp(t, &config.Config{
		Routing: config.Routing{DefaultUpstream: "frontend", SystemUpstream: "frontend"},
		Upstreams: config.UpstreamList{{
			Name:   "frontend",
			Cloud:  true,
			Listen: frontend.Listen(),
		}},
		APITranslations: cloudAPIAt(cloud.addr),
	})

	reply, err := f.Client().ListNamespaces(
		f.Context(),
		&workflowservice.ListNamespacesRequest{PageSize: 7},
		grpc.WaitForReady(true),
	)
	require.NoError(t, err)

	// The caller's request became a CloudService one on the wire.
	got := cloud.request()
	require.NotNil(t, got, "the Cloud upstream must have been called")
	require.Equal(t, int32(7), got.GetPageSize())

	// The API version header the Cloud API requires was stamped by the proxy, not
	// by the caller, which sent none.
	require.Equal(t,
		[]string{cloudclient.DefaultAPIVersion()},
		cloud.metadata().Get(cloudclient.TemporalCloudAPIVersionHeader()),
	)

	// And the caller got a WorkflowService reply built from the Cloud response.
	require.Len(t, reply.GetNamespaces(), 1)
	ns := reply.GetNamespaces()[0]
	require.Equal(t, "payments.a1b2c", ns.GetNamespaceInfo().GetName())
	require.Equal(t, enumspb.NAMESPACE_STATE_REGISTERED, ns.GetNamespaceInfo().GetState())
	require.True(t, ns.GetIsGlobalNamespace())
}

// TestEndToEndOnlyTranslatedMethodsReachTheCloudAPI proves the capture is
// surgical. GetSystemInfo carries no namespace either, and travels the same
// upstream connection the translation is installed on, so only the registry
// distinguishes them. Every SDK calls GetSystemInfo on connect, so diverting it
// to a host that does not serve WorkflowService would break every client.
func TestEndToEndOnlyTranslatedMethodsReachTheCloudAPI(t *testing.T) {
	t.Parallel()

	frontend := dataplanetest.NewUpstream(t)
	cloud := newCloudUpstream(t)

	f := dataplanetest.StartApp(t, &config.Config{
		Routing: config.Routing{DefaultUpstream: "frontend", SystemUpstream: "frontend"},
		Upstreams: config.UpstreamList{{
			Name:   "frontend",
			Cloud:  true,
			Listen: frontend.Listen(),
		}},
		APITranslations: cloudAPIAt(cloud.addr),
	})

	_, err := f.Client().GetSystemInfo(
		f.Context(),
		&workflowservice.GetSystemInfoRequest{},
		grpc.WaitForReady(true),
	)
	require.NoError(t, err)

	require.NotNil(t, frontend.Metadata(), "GetSystemInfo must still reach the frontend")
	require.Nil(t, cloud.request(), "and must not be diverted to the Cloud API")
}

// TestEndToEndANonCloudUpstreamTranslatesNothing is the negative control for the
// test above: the same fixture with an upstream that is not Cloud. It establishes
// that being a Cloud upstream is what diverts the call, rather than it reaching
// the Cloud API by some other path.
//
// It asserts on the Cloud fake rather than on the frontend, because the frontend
// fake only records the handlers it implements: ListNamespaces falls through to
// its embedded Unimplemented stub, so "the frontend recorded nothing" would hold
// whether or not the call reached it.
func TestEndToEndANonCloudUpstreamTranslatesNothing(t *testing.T) {
	t.Parallel()

	cloud := newCloudUpstream(t)

	f := dataplanetest.StartApp(t, &config.Config{
		Routing: config.Routing{DefaultUpstream: "frontend", SystemUpstream: "frontend"},
		Upstreams: config.UpstreamList{{
			Name:   "frontend",
			Listen: dataplanetest.NewUpstream(t).Listen(),
		}},
	})

	// The frontend fake does not implement ListNamespaces, so this is the error a
	// real Temporal frontend's answer stands in for. What matters is where it came
	// from, not that it failed.
	_, err := f.Client().ListNamespaces(
		f.Context(),
		&workflowservice.ListNamespacesRequest{},
		grpc.WaitForReady(true),
	)
	require.Error(t, err)
	require.Nil(t, cloud.request(), "a non-Cloud upstream must translate nothing")
}

// newCloudUpstream starts a fake CloudService on a loopback port and stops it
// when the test ends.
func newCloudUpstream(t *testing.T) *cloudUpstream {
	t.Helper()

	up := new(cloudUpstream)

	svr := grpc.NewServer()
	cloudservice.RegisterCloudServiceServer(svr, up)

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	go func() { _ = svr.Serve(lis) }()
	t.Cleanup(svr.Stop)

	up.addr = lis.Addr().String()

	return up
}

// GetNamespaces records the call and answers with the configured page.
func (u *cloudUpstream) GetNamespaces(
	ctx context.Context, req *cloudservice.GetNamespacesRequest,
) (*cloudservice.GetNamespacesResponse, error) {
	md, _ := metadata.FromIncomingContext(ctx)

	u.mu.Lock()
	defer u.mu.Unlock()

	u.got = req
	u.md = md

	return &cloudservice.GetNamespacesResponse{Namespaces: u.page}, nil
}

// GetNamespace records the namespace it was asked for and answers with one custom
// search attribute.
func (u *cloudUpstream) GetNamespace(
	_ context.Context, req *cloudservice.GetNamespaceRequest,
) (*cloudservice.GetNamespaceResponse, error) {
	u.mu.Lock()
	defer u.mu.Unlock()

	u.getNS = req.GetNamespace()

	return &cloudservice.GetNamespaceResponse{Namespace: &cloudnamespace.Namespace{
		Namespace: req.GetNamespace(),
		Spec: &cloudnamespace.NamespaceSpec{SearchAttributes: map[string]cloudnamespace.NamespaceSpec_SearchAttributeType{
			"OrderId": cloudnamespace.NamespaceSpec_SEARCH_ATTRIBUTE_TYPE_KEYWORD,
		}},
	}}, nil
}

func (u *cloudUpstream) setPage(ns ...*cloudnamespace.Namespace) {
	u.mu.Lock()
	defer u.mu.Unlock()

	u.page = ns
}

// request is the most recent GetNamespaces request, or nil before the first one
// arrives.
func (u *cloudUpstream) request() *cloudservice.GetNamespacesRequest {
	u.mu.Lock()
	defer u.mu.Unlock()

	return u.got
}

// gotNamespace is the namespace the most recent GetNamespace asked for, or empty
// before the first one arrives.
func (u *cloudUpstream) gotNamespace() string {
	u.mu.Lock()
	defer u.mu.Unlock()

	return u.getNS
}

func (u *cloudUpstream) metadata() metadata.MD {
	u.mu.Lock()
	defer u.mu.Unlock()

	return u.md.Copy()
}

// TestEndToEndNamespaceRulesApplyToATranslatedReply pins the interceptor's
// position in the chain, which nothing else would catch.
//
// Via leaves the interceptor chain when a translation fires, so whatever sits
// below method translation never runs for a translated call. Installed
// innermost, as it is, the namespace translator above it still sees the
// converted reply and rewrites the names in it; moved any higher, the call
// leaves before the namespace translator is reached and the caller gets raw
// Cloud names instead. Both orderings compile and leave every other test
// passing.
func TestEndToEndNamespaceRulesApplyToATranslatedReply(t *testing.T) {
	t.Parallel()

	cloud := newCloudUpstream(t)
	cloud.setPage(&cloudnamespace.Namespace{
		Namespace: "payments.a1b2c",
		State:     resource.ResourceState_RESOURCE_STATE_ACTIVE,
	})

	f := dataplanetest.StartApp(t, &config.Config{
		Routing: config.Routing{DefaultUpstream: "frontend", SystemUpstream: "frontend"},
		Upstreams: config.UpstreamList{{
			Name:       "frontend",
			Cloud:      true,
			Listen:     dataplanetest.NewUpstream(t).Listen(),
			Namespaces: config.NamespaceConfig{Rules: config.NamespaceRules{Suffix: ".a1b2c"}},
		}},
		APITranslations: cloudAPIAt(cloud.addr),
	})

	reply, err := f.Client().ListNamespaces(
		f.Context(),
		&workflowservice.ListNamespacesRequest{},
		grpc.WaitForReady(true),
	)
	require.NoError(t, err)

	require.Len(t, reply.GetNamespaces(), 1)
	require.Equal(t, "payments", reply.GetNamespaces()[0].GetNamespaceInfo().GetName(),
		"the namespace translator must still see the converted reply")
}

// cloudAPIAt points method translation at a fake control plane. The fake serves
// plaintext, so it has to say so: a dialled target with no tls block verifies
// against the system roots, which is what the real address wants.
func cloudAPIAt(hostPort string) config.APITranslations {
	return config.APITranslations{
		CloudAPI: config.CloudAPI{Listen: config.ListenConfig{HostPort: hostPort, Insecure: true}},
	}
}

// TestEndToEndACloudUpstreamServingNoNamespacelessRequestsTranslatesNothing is
// the hybrid case, and what replaced the operator's on/off switch. Temporal Cloud
// serves this deployment's namespaced traffic by rule, while namespace-less
// requests are routed to a Temporal Service that answers them itself. Translation
// follows the request rather than the upstream, so nothing is translated here and
// the operator who wants Cloud's own answer back says so in routing.
func TestEndToEndACloudUpstreamServingNoNamespacelessRequestsTranslatesNothing(t *testing.T) {
	t.Parallel()

	cloud := newCloudUpstream(t)

	f := dataplanetest.StartApp(t, &config.Config{
		Routing: config.Routing{
			DefaultUpstream: "onprem",
			SystemUpstream:  "onprem",
			Rules: []config.RoutingRule{{
				Upstream: "cloud",
				Match:    config.RoutingMatch{Namespace: "*.a1b2c"},
			}},
		},
		Upstreams: config.UpstreamList{
			{Name: "onprem", Listen: dataplanetest.NewUpstream(t).Listen()},
			{Name: "cloud", Cloud: true, Listen: dataplanetest.NewUpstream(t).Listen()},
		},
		APITranslations: cloudAPIAt(cloud.addr),
	})

	_, err := f.Client().ListNamespaces(
		f.Context(),
		&workflowservice.ListNamespacesRequest{},
		grpc.WaitForReady(true),
	)
	require.Error(t, err, "the request went to the upstream serving namespace-less traffic, untranslated")
	require.Nil(t, cloud.request(), "and the control plane is never reached")
}

// TestEndToEndANamespacedTranslationFiresOnTheCloudUpstreamItLandsOn is the
// other half of the hybrid. ListSearchAttributes carries a namespace, so it is
// routed by rule to the Cloud upstream rather than to the one serving
// namespace-less requests, and must be translated there. The Cloud upstream's
// namespace rules also pin the ordering: GetNamespace must be asked for the
// remote name, which only happens when namespace translation runs before method
// translation replaces the call.
func TestEndToEndANamespacedTranslationFiresOnTheCloudUpstreamItLandsOn(t *testing.T) {
	t.Parallel()

	cloud := newCloudUpstream(t)

	f := dataplanetest.StartApp(t, &config.Config{
		Routing: config.Routing{
			DefaultUpstream: "onprem",
			SystemUpstream:  "onprem",
			Rules: []config.RoutingRule{{
				Upstream: "cloud",
				Match:    config.RoutingMatch{Namespace: "payments"},
			}},
		},
		Upstreams: config.UpstreamList{
			{Name: "onprem", Listen: dataplanetest.NewUpstream(t).Listen()},
			{
				Name:       "cloud",
				Cloud:      true,
				Listen:     dataplanetest.NewUpstream(t).Listen(),
				Namespaces: config.NamespaceConfig{Rules: config.NamespaceRules{Suffix: ".a1b2c"}},
			},
		},
		APITranslations: cloudAPIAt(cloud.addr),
	})

	reply, err := operatorservice.NewOperatorServiceClient(f.Conn()).ListSearchAttributes(
		f.Context(),
		&operatorservice.ListSearchAttributesRequest{Namespace: "payments"},
		grpc.WaitForReady(true),
	)
	require.NoError(t, err)

	require.Equal(t, "payments.a1b2c", cloud.gotNamespace(), "GetNamespace must be asked for the remote name")
	require.Equal(t,
		map[string]enumspb.IndexedValueType{"OrderId": enumspb.INDEXED_VALUE_TYPE_KEYWORD},
		reply.GetCustomAttributes(),
	)
	require.Empty(t, reply.GetSystemAttributes())
}

// TestEndToEndTheDefaultUpstreamCarriesNamespacelessRequests covers the ordinary
// single-upstream config, which names no system upstream at all. Namespace-less
// requests fall through to the default, so translation has to follow them there
// or ListNamespaces stays broken for the configuration most operators write.
func TestEndToEndTheDefaultUpstreamCarriesNamespacelessRequests(t *testing.T) {
	t.Parallel()

	cloud := newCloudUpstream(t)
	cloud.setPage(&cloudnamespace.Namespace{
		Namespace: "payments.a1b2c",
		State:     resource.ResourceState_RESOURCE_STATE_ACTIVE,
	})

	f := dataplanetest.StartApp(t, &config.Config{
		Routing: config.Routing{DefaultUpstream: "frontend"},
		Upstreams: config.UpstreamList{{
			Name:   "frontend",
			Cloud:  true,
			Listen: dataplanetest.NewUpstream(t).Listen(),
		}},
		APITranslations: cloudAPIAt(cloud.addr),
	})

	reply, err := f.Client().ListNamespaces(
		f.Context(),
		&workflowservice.ListNamespacesRequest{},
		grpc.WaitForReady(true),
	)
	require.NoError(t, err)
	require.Len(t, reply.GetNamespaces(), 1)
	require.NotNil(t, cloud.request(), "the default upstream is where a namespace-less request lands")
}
