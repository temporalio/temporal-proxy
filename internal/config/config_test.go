package config_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/temporalio/temporal-proxy/internal/config"
)

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
