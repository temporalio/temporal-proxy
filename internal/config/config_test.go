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
		require.ErrorContains(t, err, "listen.hostPort is required")
	})

	t.Run("minimal config", func(t *testing.T) {
		t.Parallel()

		yaml := `
listen:
  hostPort: "localhost:7233"
`
		cfg, err := config.Load(strings.NewReader(yaml))
		require.NoError(t, err)
		require.Equal(t, &config.Config{
			Listen: config.ListenConfig{
				HostPort: "localhost:7233",
			},
		}, cfg)
	})

	t.Run("fully configured", func(t *testing.T) {
		t.Parallel()

		yaml := `
listen:
  hostPort: "localhost:7233"
  tls:
    cert: "/path/to/cert.pem"
    key: "/path/to/key.pem"
    ca: "/path/to/ca.pem"
    serverName: "temporal.example.com"
`
		cfg, err := config.Load(strings.NewReader(yaml))
		require.NoError(t, err)
		require.Equal(t, &config.Config{
			Listen: config.ListenConfig{
				HostPort: "localhost:7233",
				TLS: &config.TLS{
					Cert:       "/path/to/cert.pem",
					Key:        "/path/to/key.pem",
					CA:         "/path/to/ca.pem",
					ServerName: "temporal.example.com",
				},
			},
		}, cfg)
	})
}
