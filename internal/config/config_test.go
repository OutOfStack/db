package config_test

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/OutOfStack/db/internal/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLoadServerConfig(t *testing.T) {
	t.Run("loads default config if file not found", func(t *testing.T) {
		cfg, err := config.LoadServerConfig("non-existent-config.yaml")
		require.NoError(t, err)
		assert.Equal(t, config.DefaultServerConfig(), cfg)
	})

	t.Run("loads config from file", func(t *testing.T) {
		configContent := `
engine:
  type: "in_memory"
network:
  address: "0.0.0.0:8080"
  max_connections: 50
  max_message_size: 8
  idle_timeout: 10m
logging:
  level: "debug"
  output: "/tmp/test.log"
`
		tmpFile, err := os.CreateTemp(".", "config_test_*.yaml")
		require.NoError(t, err)
		defer os.Remove(tmpFile.Name())

		_, err = tmpFile.WriteString(configContent)
		require.NoError(t, err)
		err = tmpFile.Close()
		require.NoError(t, err)

		cfg, err := config.LoadServerConfig(filepath.Base(tmpFile.Name()))
		require.NoError(t, err)

		assert.Equal(t, "0.0.0.0:8080", cfg.Network.Address)
		assert.Equal(t, 50, cfg.Network.MaxConnections)
		assert.Equal(t, 8, cfg.Network.MaxMessageSizeKB)
		assert.Equal(t, 10*time.Minute, cfg.Network.IdleTimeout)
		assert.Equal(t, "debug", cfg.Logging.Level)
		assert.Equal(t, "/tmp/test.log", cfg.Logging.Output)
		assert.Equal(t, "in_memory", cfg.Engine.Type)
	})

	t.Run("returns error for invalid config values", func(t *testing.T) {
		configContent := `
engine:
  type: "in_memory"
network:
  address: "localhost:1234"
  max_connections: -1
`
		tmpFile, err := os.CreateTemp(".", "config_test_*.yaml")
		require.NoError(t, err)
		defer os.Remove(tmpFile.Name())

		_, err = tmpFile.WriteString(configContent)
		require.NoError(t, err)
		err = tmpFile.Close()
		require.NoError(t, err)

		_, err = config.LoadServerConfig(filepath.Base(tmpFile.Name()))
		require.Error(t, err)
		assert.Contains(t, err.Error(), "invalid config: maxConnections must be positive")
	})
}

func TestLoadClientConfig(t *testing.T) {
	t.Run("loads default config if file not found", func(t *testing.T) {
		cfg, err := config.LoadClientConfig("non-existent-config.yaml")
		require.NoError(t, err)
		assert.Equal(t, config.DefaultClientConfig(), cfg)
	})

	t.Run("loads config from file", func(t *testing.T) {
		configContent := `
network:
  address: "192.168.1.1:1234"
  max_message_size: 16
  idle_timeout: 5m
`
		tmpFile, err := os.CreateTemp(".", "config_test_*.yaml")
		require.NoError(t, err)
		defer os.Remove(tmpFile.Name())

		_, err = tmpFile.WriteString(configContent)
		require.NoError(t, err)
		err = tmpFile.Close()
		require.NoError(t, err)

		cfg, err := config.LoadClientConfig(filepath.Base(tmpFile.Name()))
		require.NoError(t, err)

		assert.Equal(t, "192.168.1.1:1234", cfg.Network.Address)
		assert.Equal(t, 16, cfg.Network.MaxMessageSizeKB)
		assert.Equal(t, 5*time.Minute, cfg.Network.IdleTimeout)
	})

	t.Run("returns error for invalid config values", func(t *testing.T) {
		configContent := `
network:
  address: "localhost:1234"
  max_message_size: 4
  idle_timeout: -5m
`
		tmpFile, err := os.CreateTemp(".", "config_test_*.yaml")
		require.NoError(t, err)
		defer os.Remove(tmpFile.Name())

		_, err = tmpFile.WriteString(configContent)
		require.NoError(t, err)
		err = tmpFile.Close()
		require.NoError(t, err)

		_, err = config.LoadClientConfig(filepath.Base(tmpFile.Name()))
		require.Error(t, err)
		assert.Contains(t, err.Error(), "invalid config: idleTimeout must be positive")
	})
}

func TestServerConfigValidate(t *testing.T) {
	cfg := config.DefaultServerConfig()
	require.NoError(t, cfg.Validate())

	t.Run("invalid engine", func(t *testing.T) {
		c := config.DefaultServerConfig()
		c.Engine.Type = "bad"
		err := c.Validate()
		require.Error(t, err)
	})

	t.Run("invalid network values", func(t *testing.T) {
		c := config.DefaultServerConfig()
		c.Network.Address = ""
		err := c.Validate()
		require.Error(t, err)

		c = config.DefaultServerConfig()
		c.Network.MaxConnections = 0
		err = c.Validate()
		require.Error(t, err)

		c = config.DefaultServerConfig()
		c.Network.MaxMessageSizeKB = 0
		err = c.Validate()
		require.Error(t, err)

		c = config.DefaultServerConfig()
		c.Network.IdleTimeout = 0
		err = c.Validate()
		require.Error(t, err)
	})
}

func TestClientConfigValidate(t *testing.T) {
	cfg := config.DefaultClientConfig()
	require.NoError(t, cfg.Validate())

	t.Run("invalid network values", func(t *testing.T) {
		c := config.DefaultClientConfig()
		c.Network.Address = ""
		err := c.Validate()
		require.Error(t, err)

		c = config.DefaultClientConfig()
		c.Network.MaxMessageSizeKB = -1
		err = c.Validate()
		require.Error(t, err)

		c = config.DefaultClientConfig()
		c.Network.IdleTimeout = 0
		err = c.Validate()
		require.Error(t, err)
	})
}
