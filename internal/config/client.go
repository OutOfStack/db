package config

import (
	"errors"
	"time"

	"github.com/OutOfStack/db/internal/pool"
)

// ClientConfig - configuration for the database client
type ClientConfig struct {
	Network ClientNetworkConfig `yaml:"network"`
	Pool    pool.PoolConfig     `yaml:"pool"`
}

// ClientNetworkConfig - network-related configuration for the database client
type ClientNetworkConfig struct {
	Address          string        `yaml:"address"`
	MaxMessageSizeKB int           `yaml:"max_message_size"`
	IdleTimeout      time.Duration `yaml:"idle_timeout"`
}

// DefaultClientConfig returns a ClientConfig instance with sensible default values.
// This is used as a fallback when no configuration file is provided or when
// certain configuration parameters are missing
func DefaultClientConfig() *ClientConfig {
	return &ClientConfig{
		Network: ClientNetworkConfig{
			Address:          "127.0.0.1:3223",
			MaxMessageSizeKB: 4,
			IdleTimeout:      time.Minute,
		},
		Pool: *pool.DefaultPoolConfig(),
	}
}

// Validate checks if the configuration values are valid
func (c *ClientConfig) Validate() error {
	// If pool is enabled, validate pool config instead of single address
	if c.Pool.Enabled {
		if err := c.Pool.Validate(); err != nil {
			return err
		}
	} else {
		// Validate single server config
		if c.Network.Address == "" {
			return errors.New("network address cannot be empty")
		}
	}

	if c.Network.MaxMessageSizeKB <= 0 {
		return errors.New("maxMessageSize must be positive")
	}

	if c.Network.IdleTimeout <= 0 {
		return errors.New("idleTimeout must be positive")
	}

	return nil
}
