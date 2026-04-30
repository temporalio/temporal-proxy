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

func TestLoad(t *testing.T) {
	t.Parallel()

	t.Run("invalid YAML returns error", func(t *testing.T) {
		t.Parallel()

		_, err := config.Load(strings.NewReader(":::"))
		require.Error(t, err)
	})

	t.Run("missing required field returns error", func(t *testing.T) {
		t.Parallel()

		_, err := config.Load(strings.NewReader("{}"))
		require.ErrorContains(t, err, "must define at least one cluster")
	})

	t.Run("minimal config", func(t *testing.T) {
		t.Parallel()

		yaml := `
clusters:
  - type: inbound
    listener:
      hostPort: :8233
`
		cfg, err := config.Load(strings.NewReader(yaml))
		require.NoError(t, err)
		require.Equal(t, &config.Config{
			Clusters: []config.Cluster{
				{
					Type:     config.Inbound,
					Listener: config.ListenConfig{HostPort: ":8233"},
				},
			},
		}, cfg)
	})

	t.Run("fully configured", func(t *testing.T) {
		t.Parallel()

		yaml := `
metrics:
  hostPort: :9210
  path: /metrikz
clusters:
  - name: temporal-cloud
    type: temporal
    listener:
      hostPort: :8233
      tls:
        cert: "/path/to/cert.pem"
        key: "/path/to/key.pem"
        ca: "/path/to/ca.pem"
        serverName: "temporal.example.com"
    upstream:
      poolSize: 5
      hostPort: :10233
      tls:
        cert: "/path/to/cert.pem"
        key: "/path/to/key.pem"
        ca: "/path/to/ca.pem"
        serverName: "temporal.example.com"
`
		cfg, err := config.Load(strings.NewReader(yaml))
		require.NoError(t, err)
		require.Equal(t, &config.Config{
			Metrics: config.MetricsConfig{
				HostPort: ":9210",
				Path:     "/metrikz",
			},
			Clusters: []config.Cluster{
				{
					Name: "temporal-cloud",
					Type: config.Temporal,
					Listener: config.ListenConfig{
						HostPort: ":8233",
						TLS: &config.TLS{
							Cert:       "/path/to/cert.pem",
							Key:        "/path/to/key.pem",
							CA:         "/path/to/ca.pem",
							ServerName: "temporal.example.com",
						},
					},
					Upstream: config.Upstream{
						PoolSize: 5,
						Listener: config.ListenConfig{
							HostPort: ":10233",
							TLS: &config.TLS{
								Cert:       "/path/to/cert.pem",
								Key:        "/path/to/key.pem",
								CA:         "/path/to/ca.pem",
								ServerName: "temporal.example.com",
							},
						},
					},
				},
			},
		}, cfg)
	})
}
