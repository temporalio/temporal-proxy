package proxy_test

import (
	"bytes"
	"context"
	"encoding/base64"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/stretchr/testify/require"
	"go.temporal.io/api/common/v1"
	"go.temporal.io/api/failure/v1"
	"go.temporal.io/api/history/v1"
	"go.temporal.io/api/nexus/v1"
	workflowservice "go.temporal.io/api/workflowservice/v1"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"

	"github.com/temporalio/temporal-proxy/internal/metrics"
	"github.com/temporalio/temporal-proxy/internal/proxy"
	"github.com/temporalio/temporal-proxy/internal/transport/meta"
	"github.com/temporalio/temporal-proxy/pkg/codec"
	"github.com/temporalio/temporal-proxy/pkg/crypto"
)

// TestEncryptionSealsUnderTheRemoteNamespace pins the label a sealed payload
// carries. Labeling with the local name fails the translated row; translating
// an absent namespace fails the empty row, since Remote("") is the bare suffix.
func TestEncryptionSealsUnderTheRemoteNamespace(t *testing.T) {
	t.Parallel()

	suffix := proxy.WithRemoteNamespace(func(ns string) string { return ns + ".acct" })

	tests := []struct {
		name  string
		local string
		opts  []proxy.InterceptorOption
		want  string
	}{
		{name: "translated", local: "orders", opts: []proxy.InterceptorOption{suffix}, want: "orders.acct"},
		{name: "empty namespace stays empty", local: "", opts: []proxy.InterceptorOption{suffix}, want: ""},
		{name: "untranslated", local: "orders", want: "orders"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			sealed := sealThroughInterceptor(t, realVault(t, crypto.BindingPermissive), tc.local, tc.opts...)

			label, ok := sealed.GetMetadata()[codec.MetadataEncryptionNamespace]
			require.True(t, ok)
			require.Equal(t, tc.want, string(label))
		})
	}
}

// TestEncryptionComparesTheRemoteNamespace pins the comparison on the gRPC path.
// Skipping ExpectNamespace fails the strict mismatch row; not counting
// mismatches fails both mismatch rows; removing the ErrNamespaceMismatch case
// from resultLabel fails the strict mismatch row's vault_ops check.
func TestEncryptionComparesTheRemoteNamespace(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		mode       crypto.BindingMode
		sealedIn   string
		wantErr    bool
		wantCount  bool
		wantResult string
	}{
		{name: "strict match", mode: crypto.BindingStrict, sealedIn: "orders", wantResult: "success"},
		{name: "strict mismatch", mode: crypto.BindingStrict, sealedIn: "billing", wantErr: true, wantCount: true, wantResult: "mismatch"},
		{name: "permissive mismatch", mode: crypto.BindingPermissive, sealedIn: "billing", wantCount: true, wantResult: "success"},
	}

	suffix := proxy.WithRemoteNamespace(func(ns string) string { return ns + ".acct" })

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			v := realVault(t, tc.mode)
			theirs := sealThroughInterceptor(t, realVault(t, crypto.BindingPermissive), tc.sealedIn, suffix)

			reg := prometheus.NewRegistry()
			interceptor, err := proxy.CodecInterceptor(
				proxy.CodecOptions{Vault: v, Reporter: bindingReporter(reg)},
				suffix,
			)
			require.NoError(t, err)

			resp := &workflowservice.StartWorkflowExecutionRequest{}
			err = interceptor(meta.WithNamespace(t.Context(), "orders"), "/method", startRequest(), resp, nil, respondWith(theirs))

			if tc.wantErr {
				require.ErrorContains(t, err, "different namespace")
			} else {
				require.NoError(t, err)
				require.Equal(t, `"x"`, string(resp.Input.Payloads[0].GetData()))
			}

			mismatches := gatherFamily(t, reg, "proxy_encryption_vault_namespace_mismatches_total")
			if tc.wantCount {
				require.NotNil(t, mismatches)
				require.True(t, hasLabels(mismatches, map[string]string{"namespace": "orders"}))
			} else {
				require.Nil(t, mismatches)
			}

			ops := gatherFamily(t, reg, "proxy_encryption_vault_ops_total")
			require.True(t, hasLabels(ops, map[string]string{
				"operation": "decrypt", "result": tc.wantResult, "namespace": "orders",
			}))
		})
	}
}

// TestEncryptionExemptsNexusPayloads pins that payloads under a Nexus message,
// including inside a failure nested in one, skip the comparison while other
// payloads in the same response do not. Dropping the context hook fails the
// Nexus rows; exempting by anything wider than Nexus fails the signal row.
func TestEncryptionExemptsNexusPayloads(t *testing.T) {
	t.Parallel()

	theirs := func(t *testing.T) *common.Payload {
		t.Helper()
		return sealThroughInterceptor(t, realVault(t, crypto.BindingPermissive), "billing")
	}

	historyWith := func(attrs *history.HistoryEvent) *workflowservice.GetWorkflowExecutionHistoryResponse {
		return &workflowservice.GetWorkflowExecutionHistoryResponse{
			History: &history.History{Events: []*history.HistoryEvent{attrs}},
		}
	}

	tests := []struct {
		name    string
		reply   func(t *testing.T) proto.Message
		wantErr bool
	}{
		{
			name: "nexus start operation request",
			reply: func(t *testing.T) proto.Message {
				return &workflowservice.PollNexusTaskQueueResponse{Request: &nexus.Request{
					Variant: &nexus.Request_StartOperation{StartOperation: &nexus.StartOperationRequest{Payload: theirs(t)}},
				}}
			},
		},
		{
			name: "failure nested in a nexus operation failed event",
			reply: func(t *testing.T) proto.Message {
				return historyWith(&history.HistoryEvent{
					Attributes: &history.HistoryEvent_NexusOperationFailedEventAttributes{
						NexusOperationFailedEventAttributes: &history.NexusOperationFailedEventAttributes{
							Failure: &failure.Failure{
								Message: "boom",
								FailureInfo: &failure.Failure_ApplicationFailureInfo{
									ApplicationFailureInfo: &failure.ApplicationFailureInfo{
										Details: &common.Payloads{Payloads: []*common.Payload{theirs(t)}},
									},
								},
							},
						},
					},
				})
			},
		},
		{
			name: "signal from another namespace is not exempt",
			reply: func(t *testing.T) proto.Message {
				return historyWith(&history.HistoryEvent{
					Attributes: &history.HistoryEvent_WorkflowExecutionSignaledEventAttributes{
						WorkflowExecutionSignaledEventAttributes: &history.WorkflowExecutionSignaledEventAttributes{
							SignalName: "s",
							Input:      &common.Payloads{Payloads: []*common.Payload{theirs(t)}},
						},
					},
				})
			},
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			interceptor, err := proxy.CodecInterceptor(proxy.CodecOptions{
				Vault:    realVault(t, crypto.BindingStrict),
				Reporter: bindingReporter(prometheus.NewRegistry()),
			})
			require.NoError(t, err)

			want := tc.reply(t)
			reply := proto.Clone(want)
			proto.Reset(reply)
			invoker := func(_ context.Context, _ string, _, got any, _ *grpc.ClientConn, _ ...grpc.CallOption) error {
				proto.Merge(got.(proto.Message), want)
				return nil
			}

			err = interceptor(meta.WithNamespace(t.Context(), "orders"), "/method", startRequest(), reply, nil, invoker)
			if tc.wantErr {
				require.ErrorContains(t, err, "different namespace")
				return
			}

			require.NoError(t, err)
		})
	}
}

// TestCodecsDecodeDoesNotCompareNamespaces pins that the codec server's path
// opens a payload bound elsewhere even under strict, until /decode compares
// namespaces itself. Expecting the namespace there fails this test.
func TestCodecsDecodeDoesNotCompareNamespaces(t *testing.T) {
	t.Parallel()

	codecs, err := proxy.NewCodecs(proxy.CodecOptions{
		Vault:    realVault(t, crypto.BindingStrict),
		Reporter: bindingReporter(prometheus.NewRegistry()),
	})
	require.NoError(t, err)

	theirs := sealThroughInterceptor(t, realVault(t, crypto.BindingPermissive), "billing")

	opened, err := codecs.Decode(t.Context(), "orders", []*common.Payload{theirs})
	require.NoError(t, err)
	require.Equal(t, `"x"`, string(opened[0].GetData()))
}

// sealThroughInterceptor seals one json payload through an encrypting
// interceptor on v for a request in local, and returns it as the upstream
// received it.
func sealThroughInterceptor(t *testing.T, v proxy.Vault, local string, opts ...proxy.InterceptorOption) *common.Payload {
	t.Helper()

	interceptor, err := proxy.CodecInterceptor(
		proxy.CodecOptions{Vault: v, Encrypt: true, Reporter: bindingReporter(prometheus.NewRegistry())},
		opts...,
	)
	require.NoError(t, err)

	var sent *common.Payload
	invoker := func(_ context.Context, _ string, req, _ any, _ *grpc.ClientConn, _ ...grpc.CallOption) error {
		sent = req.(*workflowservice.StartWorkflowExecutionRequest).Input.Payloads[0]
		return nil
	}

	ctx := meta.WithNamespace(t.Context(), local)
	require.NoError(t, interceptor(ctx, "/method", startRequest(testPayload("json/plain", `"x"`)),
		&workflowservice.StartWorkflowExecutionRequest{}, nil, invoker))
	require.Equal(t, codec.EncryptionEncoding, string(sent.GetMetadata()[codec.MetadataEncoding]))

	return sent
}

// realVault builds a vault on a fixed testing key, so vaults built separately in
// one test open each other's payloads.
func realVault(t *testing.T, mode crypto.BindingMode) *crypto.Vault {
	t.Helper()

	uri := "testing://" + base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{0x2a}, 32))
	kek, err := crypto.NewCloudKey(t.Context(), uri)
	require.NoError(t, err)
	t.Cleanup(func() { _ = kek.Close() })

	reg, err := crypto.NewKEKRegistry(crypto.WithDefaultKey(kek))
	require.NoError(t, err)

	v, err := crypto.NewVault(reg,
		crypto.WithDefaultKeyConfig(crypto.KeyConfig{Duration: time.Hour}),
		crypto.WithBindingMode(mode))
	require.NoError(t, err)

	return v
}

// bindingReporter builds a reporter with namespace labels on, recording into reg.
func bindingReporter(reg *prometheus.Registry) *proxy.Reporter {
	return proxy.NewReporter(
		metrics.New("proxy", promauto.With(reg)).ForSubsystem("encryption"),
		proxy.WithNamespaceLabels(true),
	)
}
