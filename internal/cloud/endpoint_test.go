package cloud_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/temporalio/temporal-proxy/internal/cloud"
)

func TestIsEndpoint(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		hostPort string
		want     bool
	}{
		{name: "per-namespace endpoint", hostPort: "quickstart.a1b2c.tmprl.cloud:7233", want: true},
		{name: "regional endpoint", hostPort: "us-west-2.aws.tmprl.cloud:7233", want: true},
		{name: "templated host", hostPort: "{{ .RemoteNamespace }}.tmprl.cloud:7233", want: true},
		{name: "no port", hostPort: "quickstart.a1b2c.tmprl.cloud", want: true},
		{name: "self-hosted", hostPort: "localhost:7233"},
		{name: "private-link hostname", hostPort: "vpce-0abc123.vpce-svc-0def456.us-east-1.vpce.amazonaws.com:7233"},
		{name: "lookalike domain", hostPort: "evil-tmprl.cloud:7233"},
		{name: "bare domain", hostPort: "tmprl.cloud:7233"},
		{name: "empty"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			require.Equal(t, tt.want, cloud.IsEndpoint(tt.hostPort))
		})
	}
}
