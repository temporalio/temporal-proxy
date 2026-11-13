package e2e

import (
	"crypto/tls"
	"testing"

	"github.com/stretchr/testify/require"
	operatorservice "go.temporal.io/api/operatorservice/v1"
	"go.temporal.io/api/workflowservice/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"

	"go.temporal.io/proxy/internal/config"
	"go.temporal.io/proxy/internal/dataplane/dataplanetest"
	"go.temporal.io/proxy/pkg/testutil"
)

// TestTemplatedUpstreamsRenderingOneTargetKeepTheirOwnCredentials proves two
// templated upstreams whose templates render the same target each send their
// own credential. Their templates differ only in spacing, which is enough to
// pass the per-upstream hostPort uniqueness check while rendering identically.
func TestTemplatedUpstreamsRenderingOneTargetKeepTheirOwnCredentials(t *testing.T) {
	t.Parallel()

	up := dataplanetest.NewTLSUpstream(t)

	listen := func(hostPort string) config.ListenConfig {
		l := up.Listen()
		l.HostPort = hostPort

		return l
	}

	f := dataplanetest.Start(t, &config.Config{
		Routing: config.Routing{
			DefaultUpstream: "a",
			Rules: []config.RoutingRule{
				{Upstream: "a", Match: config.RoutingMatch{Namespace: "ns-a"}},
				{Upstream: "b", Match: config.RoutingMatch{Namespace: "ns-b"}},
			},
		},
		Upstreams: config.UpstreamList{
			{
				Name:        "a",
				Listen:      listen(`{{ index .Metadata "x-upstream" }}`),
				Credentials: &config.CredentialConfig{Static: &config.StaticCredentialConfig{APIKey: "key-a"}},
			},
			{
				Name:        "b",
				Listen:      listen(`{{index .Metadata "x-upstream"}}`),
				Credentials: &config.CredentialConfig{Static: &config.StaticCredentialConfig{APIKey: "key-b"}},
			},
		},
	})

	query := func(ns string) metadata.MD {
		ctx := metadata.AppendToOutgoingContext(f.Context(), "x-upstream", up.Addr())
		_, err := f.Client().QueryWorkflow(ctx, &workflowservice.QueryWorkflowRequest{Namespace: ns}, grpc.WaitForReady(true))
		require.NoError(t, err)

		return up.Metadata()
	}

	require.Equal(t, []string{"Bearer key-a"}, query("ns-a").Get("authorization"))
	require.Equal(t, []string{"Bearer key-b"}, query("ns-b").Get("authorization"))
}

// TestCloudUpstreamsSharingAControlPlaneKeepTheirOwnCredentials proves two Cloud
// upstreams each reach the Cloud API with their own credential. Every Cloud
// upstream's control-plane connection dials the same address, so only their
// connection scopes keep them apart.
func TestCloudUpstreamsSharingAControlPlaneKeepTheirOwnCredentials(t *testing.T) {
	t.Parallel()

	caFile, certFile, keyFile := testutil.GenerateMTLSCerts(t)
	serverCert, err := tls.LoadX509KeyPair(certFile, keyFile)
	require.NoError(t, err)

	cloud := newCloudUpstream(t, grpc.Creds(credentials.NewTLS(&tls.Config{
		Certificates: []tls.Certificate{serverCert},
		MinVersion:   tls.VersionTLS12,
	})))

	cloudUpstream := func(name, key string) config.Upstream {
		return config.Upstream{
			Name:        name,
			Cloud:       true,
			Listen:      dataplanetest.NewTLSUpstream(t).Listen(),
			Namespaces:  config.NamespaceConfig{Rules: config.NamespaceRules{Suffix: ".a1b2c"}},
			Credentials: &config.CredentialConfig{Static: &config.StaticCredentialConfig{APIKey: key}},
		}
	}

	f := dataplanetest.Start(t, &config.Config{
		Routing: config.Routing{
			DefaultUpstream: "a",
			Rules: []config.RoutingRule{
				{Upstream: "a", Match: config.RoutingMatch{Namespace: "payments"}},
				{Upstream: "b", Match: config.RoutingMatch{Namespace: "orders"}},
			},
		},
		Upstreams: config.UpstreamList{cloudUpstream("a", "key-a"), cloudUpstream("b", "key-b")},
		APITranslations: config.APITranslations{CloudAPI: config.CloudAPI{Listen: config.ListenConfig{
			HostPort: cloud.addr,
			TLS:      &config.TLSConfig{CA: caFile, ServerName: "localhost"},
		}}},
	})

	attributes := func(ns string) metadata.MD {
		_, err := operatorservice.NewOperatorServiceClient(f.Conn()).ListSearchAttributes(
			f.Context(),
			&operatorservice.ListSearchAttributesRequest{Namespace: ns},
			grpc.WaitForReady(true),
		)
		require.NoError(t, err)

		return cloud.metadata()
	}

	require.Equal(t, []string{"Bearer key-a"}, attributes("payments").Get("authorization"))
	require.Equal(t, []string{"Bearer key-b"}, attributes("orders").Get("authorization"))
}
