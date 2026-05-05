package config_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/temporalio/temporal-proxy/internal/config"
)

func TestLoad_ClusterTypeValidation(t *testing.T) {
	t.Parallel()

	t.Run("missing cluster type returns error", func(t *testing.T) {
		t.Parallel()

		yaml := `
clusters:
  - listener:
      hostPort: :8233
`
		_, err := config.Load(strings.NewReader(yaml))
		require.ErrorContains(t, err, "type is required")
	})

	t.Run("unknown cluster type returns error", func(t *testing.T) {
		t.Parallel()

		yaml := `
clusters:
  - type: bogus
    listener:
      hostPort: :8233
    upstream:
      hostPort: :9233
`
		_, err := config.Load(strings.NewReader(yaml))
		require.ErrorContains(t, err, "unknown cluster type")
	})

	t.Run("outbound requires upstream hostPort", func(t *testing.T) {
		t.Parallel()

		yaml := `
clusters:
  - type: outbound
    listener:
      hostPort: :7233
`
		_, err := config.Load(strings.NewReader(yaml))
		require.ErrorContains(t, err, "upstream.hostPort required")
	})

	t.Run("outbound requires listener hostPort", func(t *testing.T) {
		t.Parallel()

		yaml := `
clusters:
  - type: outbound
    listener: {}
    upstream:
      hostPort: :9233
`
		_, err := config.Load(strings.NewReader(yaml))
		require.ErrorContains(t, err, "listener.hostPort required")
	})

	t.Run("inbound requires listener hostPort", func(t *testing.T) {
		t.Parallel()

		yaml := `
clusters:
  - type: inbound
    listener: {}
`
		_, err := config.Load(strings.NewReader(yaml))
		require.ErrorContains(t, err, "listener.hostPort required")
	})

	t.Run("temporal requires listener hostPort", func(t *testing.T) {
		t.Parallel()

		yaml := `
clusters:
  - type: temporal
    listener: {}
    upstream:
      hostPort: my-ns.tmprl.cloud:7233
`
		_, err := config.Load(strings.NewReader(yaml))
		require.ErrorContains(t, err, "listener.hostPort required")
	})

	t.Run("temporal requires listener and upstream hostPort", func(t *testing.T) {
		t.Parallel()

		yaml := `
clusters:
  - type: temporal
    listener:
      hostPort: :8233
`
		_, err := config.Load(strings.NewReader(yaml))
		require.ErrorContains(t, err, "upstream.hostPort required")
	})

	t.Run("valid temporal config", func(t *testing.T) {
		t.Parallel()

		yaml := `
clusters:
  - name: temporal-cloud
    type: temporal
    listener:
      hostPort: :8233
    upstream:
      hostPort: my-ns.tmprl.cloud:7233
`
		cfg, err := config.Load(strings.NewReader(yaml))
		require.NoError(t, err)
		require.Equal(t, config.Temporal, cfg.Clusters[0].Type)
	})
}
