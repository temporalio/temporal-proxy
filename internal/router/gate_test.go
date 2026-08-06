package router_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/temporalio/temporal-proxy/internal/router"
	"github.com/temporalio/temporal-proxy/internal/services"
)

func TestGateAllows(t *testing.T) {
	t.Parallel()

	gate := router.NewGate([]string{services.WorkflowService, services.Reflection})

	tests := []struct {
		name    string
		service string
		want    bool
	}{
		{name: "listed service", service: services.WorkflowService, want: true},
		{name: "unlisted service", service: services.OperatorService, want: false},
		{name: "reflection", service: services.Reflection, want: true},
		{name: "reflection alias comes along", service: services.ReflectionV1Alpha, want: true},
		{name: "unknown service", service: "some.other.Service", want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tt.want, gate.Allows(tt.service))
		})
	}
}

func TestServiceOf(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		method string
		want   string
	}{
		{
			name:   "full method",
			method: "/temporal.api.workflowservice.v1.WorkflowService/GetSystemInfo",
			want:   services.WorkflowService,
		},
		{
			name:   "no leading slash",
			method: "temporal.api.workflowservice.v1.WorkflowService/GetSystemInfo",
			want:   services.WorkflowService,
		},
		{name: "malformed", method: "nonsense", want: ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tt.want, router.ServiceOf(tt.method))
		})
	}
}
