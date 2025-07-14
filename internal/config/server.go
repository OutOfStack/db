package config

import (
	"errors"
	"fmt"
	"time"

	"github.com/OutOfStack/db/internal/engine"
)

// ServerConfig - configuration for the database server
type ServerConfig struct {
	Engine  ServerEngineConfig  `yaml:"engine"`
	Network ServerNetworkConfig `yaml:"network"`
	Logging ServerLoggingConfig `yaml:"logging"`
}

// ServerEngineConfig holds configuration for the database engine.
// Currently only supports "in_memory" type
type ServerEngineConfig struct {
	Type string `yaml:"type"`
}

// ServerNetworkConfig - network-related configuration for the database server
type ServerNetworkConfig struct {
	Address          string        `yaml:"address"`
	MaxConnections   int           `yaml:"max_connections"`
	MaxMessageSizeKB int           `yaml:"max_message_size"`
	IdleTimeout      time.Duration `yaml:"idle_timeout"`
}

// ServerLoggingConfig - logging configuration including log level and output destination.
// Level can be "debug", "info", "warn", or "error". Output can be empty for stdout or a file path
type ServerLoggingConfig struct {
	Level  string `yaml:"level"`
	Output string `yaml:"output"`
}

// DefaultServerConfig returns a serverConfig instance with sensible default values.
// This is used as a fallback when no configuration file is provided or when
// certain configuration parameters are missing
func DefaultServerConfig() *ServerConfig {
	return &ServerConfig{
		Engine: ServerEngineConfig{
			Type: engine.TypeInMemory,
		},
		Network: ServerNetworkConfig{
			Address:          "127.0.0.1:3223",
			MaxConnections:   100,
			MaxMessageSizeKB: 4,
			IdleTimeout:      1 * time.Minute,
		},
		Logging: ServerLoggingConfig{
			Level:  "info",
			Output: "",
		},
	}
}

// Validate checks if the configuration values are valid
func (c *ServerConfig) Validate() error {
	if c.Engine.Type != engine.TypeInMemory {
		return fmt.Errorf("unsupported engine type: %s", c.Engine.Type)
	}

	if c.Network.Address == "" {
		return errors.New("network address cannot be empty")
	}

	if c.Network.MaxConnections <= 0 {
		return errors.New("maxConnections must be positive")
	}

	if c.Network.MaxMessageSizeKB <= 0 {
		return errors.New("maxMessageSize must be positive")
	}

	if c.Network.IdleTimeout <= 0 {
		return errors.New("idleTimeout must be positive")
	}

	return nil
}
