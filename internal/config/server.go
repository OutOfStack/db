package config

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/OutOfStack/db/internal/engine"
	"github.com/OutOfStack/db/internal/wal"
)

// Environment variables that override server configuration values
const (
	envAddress        = "DB_ADDRESS"
	envMaxConnections = "DB_MAX_CONNECTIONS"
	envMaxMessageSize = "DB_MAX_MESSAGE_SIZE"
	envIdleTimeout    = "DB_IDLE_TIMEOUT"
	envLogLevel       = "DB_LOG_LEVEL"
	envLogOutput      = "DB_LOG_OUTPUT"
)

// ServerConfig - configuration for the database server
type ServerConfig struct {
	Engine  ServerEngineConfig  `yaml:"engine"`
	WAL     ServerWALConfig     `yaml:"wal"`
	Network ServerNetworkConfig `yaml:"network"`
	Logging ServerLoggingConfig `yaml:"logging"`
}

// ServerWALConfig controls durable write-ahead logging and snapshots.
// SegmentSizeMB is measured in MiB.
type ServerWALConfig struct {
	Enabled          bool           `yaml:"enabled"`
	DataDir          string         `yaml:"data_dir"`
	Sync             wal.SyncPolicy `yaml:"sync"`
	SegmentSizeMB    int64          `yaml:"segment_size"`
	SnapshotInterval time.Duration  `yaml:"snapshot_interval"`
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
		WAL: ServerWALConfig{
			Enabled:          false,
			DataDir:          "data",
			Sync:             wal.SyncEverySec,
			SegmentSizeMB:    64,
			SnapshotInterval: 5 * time.Minute,
		},
		Network: ServerNetworkConfig{
			Address:          defaultAddress,
			MaxConnections:   100,
			MaxMessageSizeKB: 4,
			IdleTimeout:      time.Minute,
		},
		Logging: ServerLoggingConfig{
			Level:  "info",
			Output: "",
		},
	}
}

// applyEnvOverrides overrides configuration values from DB_* environment
// variables. Environment variables take precedence over file values
func (c *ServerConfig) applyEnvOverrides() error {
	if v := os.Getenv(envAddress); v != "" {
		c.Network.Address = v
	}
	if v := os.Getenv(envMaxConnections); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			return fmt.Errorf("invalid %s: %w", envMaxConnections, err)
		}
		c.Network.MaxConnections = n
	}
	if v := os.Getenv(envMaxMessageSize); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			return fmt.Errorf("invalid %s: %w", envMaxMessageSize, err)
		}
		c.Network.MaxMessageSizeKB = n
	}
	if v := os.Getenv(envIdleTimeout); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			return fmt.Errorf("invalid %s: %w", envIdleTimeout, err)
		}
		c.Network.IdleTimeout = d
	}
	if v := os.Getenv(envLogLevel); v != "" {
		c.Logging.Level = v
	}
	if v := os.Getenv(envLogOutput); v != "" {
		c.Logging.Output = v
	}
	return nil
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

	if c.WAL.Enabled {
		if c.WAL.DataDir == "" {
			return errors.New("wal dataDir cannot be empty")
		}
		switch c.WAL.Sync {
		case wal.SyncAlways, wal.SyncEverySec, wal.SyncNo:
		default:
			return fmt.Errorf("unsupported wal sync policy: %s", c.WAL.Sync)
		}
		if c.WAL.SegmentSizeMB <= 0 {
			return errors.New("wal segmentSize must be positive")
		}
		if c.WAL.SnapshotInterval <= 0 {
			return errors.New("wal snapshotInterval must be positive")
		}
	}

	return nil
}
