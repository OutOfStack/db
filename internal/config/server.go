package config

import (
	"errors"
	"fmt"
	"math"
	"os"
	"strconv"
	"time"

	"github.com/OutOfStack/db/internal/engine"
	"github.com/OutOfStack/db/internal/wal"
)

// maxMB is the largest megabyte value that converts to bytes (mb << 20) without overflowing an int64.
const maxMB = math.MaxInt64 / (1 << 20)

// Environment variables that override server configuration values
const (
	envAddress        = "DB_ADDRESS"
	envMaxConnections = "DB_MAX_CONNECTIONS"
	envMaxMessageSize = "DB_MAX_MESSAGE_SIZE"
	envIdleTimeout    = "DB_IDLE_TIMEOUT"
	envLogLevel       = "DB_LOG_LEVEL"
	envLogOutput      = "DB_LOG_OUTPUT"
)

const defaultLogLevel = "info"

// Replication roles.
const (
	RoleStandalone = ""
	RoleMaster     = "master"
	RoleStandby    = "standby"
)

// ServerConfig - configuration for the database server
type ServerConfig struct {
	Engine      ServerEngineConfig      `yaml:"engine"`
	WAL         ServerWALConfig         `yaml:"wal"`
	Replication ServerReplicationConfig `yaml:"replication"`
	Network     ServerNetworkConfig     `yaml:"network"`
	Logging     ServerLoggingConfig     `yaml:"logging"`
}

// ServerReplicationConfig controls master/standby log shipping. Role is "master", "standby", or empty (standalone). A
// master streams its WAL to standbys that connect to ListenAddress; a standby connects to MasterAddress. Replication
// requires WAL persistence to be enabled.
type ServerReplicationConfig struct {
	Role             string        `yaml:"role"`
	ListenAddress    string        `yaml:"listen_address"`
	MasterAddress    string        `yaml:"master_address"`
	ReconnectBackoff time.Duration `yaml:"reconnect_backoff"`
	// AllowRemotePromote permits the PROMOTE command over the client port. Off by default: promotion changes which node
	// accepts writes, so it has to be an explicit operator decision.
	AllowRemotePromote bool `yaml:"allow_remote_promote"`
}

// ServerWALConfig controls durable write-ahead logging and snapshots. SegmentSizeMB is measured in MiB.
type ServerWALConfig struct {
	Enabled          bool           `yaml:"enabled"`
	DataDir          string         `yaml:"data_dir"`
	Sync             wal.SyncPolicy `yaml:"sync"`
	SegmentSizeMB    int64          `yaml:"segment_size"`
	SnapshotInterval time.Duration  `yaml:"snapshot_interval"`
}

// ServerEngineConfig holds configuration for the database engine. Type is "in_memory" (RAM-only) or "tiered"
// (memory/disk). The Tiered* fields apply only to the tiered engine, which provides its own durability and therefore
// cannot be combined with the WAL or replication.
type ServerEngineConfig struct {
	Type                string         `yaml:"type"`
	DataDir             string         `yaml:"data_dir"`
	MaxMemoryMB         int64          `yaml:"max_memory"`           // hot values kept in RAM (MiB)
	MaxStorageMB        int64          `yaml:"max_storage"`          // live dataset ceiling (MiB)
	Sync                wal.SyncPolicy `yaml:"sync"`                 // fsync policy for segments
	SegmentSizeMB       int64          `yaml:"segment_size"`         // segment file size (MiB)
	CompactionThreshold float64        `yaml:"compaction_threshold"` // reclaim a segment past this dead-bytes ratio
	CompactionInterval  time.Duration  `yaml:"compaction_interval"`  // compaction/stats check period
}

// ServerNetworkConfig - network-related configuration for the database server
type ServerNetworkConfig struct {
	Address          string        `yaml:"address"`
	MaxConnections   int           `yaml:"max_connections"`
	MaxMessageSizeKB int           `yaml:"max_message_size"`
	IdleTimeout      time.Duration `yaml:"idle_timeout"`
	ShutdownTimeout  time.Duration `yaml:"shutdown_timeout"`
}

// ServerLoggingConfig - logging configuration including log level and output destination. Level can be "debug", "info",
// "warn", or "error". Output can be empty for stdout or a file path
type ServerLoggingConfig struct {
	Level  string `yaml:"level"`
	Output string `yaml:"output"`
}

// DefaultServerConfig returns a serverConfig instance with sensible default values. This is used as a fallback when no
// configuration file is provided or when certain configuration parameters are missing
func DefaultServerConfig() *ServerConfig {
	return &ServerConfig{
		Engine: ServerEngineConfig{
			Type:                engine.TypeInMemory,
			DataDir:             defaultDataDir,
			MaxMemoryMB:         64,
			MaxStorageMB:        1024,
			Sync:                wal.SyncEverySec,
			SegmentSizeMB:       64,
			CompactionThreshold: 0.5,
			CompactionInterval:  30 * time.Second,
		},
		WAL: ServerWALConfig{
			Enabled:          false,
			DataDir:          defaultDataDir,
			Sync:             wal.SyncEverySec,
			SegmentSizeMB:    64,
			SnapshotInterval: 5 * time.Minute,
		},
		Replication: ServerReplicationConfig{
			Role:             RoleStandalone,
			ReconnectBackoff: time.Second,
		},
		Network: ServerNetworkConfig{
			Address:          defaultAddress,
			MaxConnections:   100,
			MaxMessageSizeKB: 4,
			IdleTimeout:      time.Minute,
			ShutdownTimeout:  10 * time.Second,
		},
		Logging: ServerLoggingConfig{
			Level:  defaultLogLevel,
			Output: "",
		},
	}
}

// applyEnvOverrides overrides configuration values from DB_* environment variables. Environment variables take
// precedence over file values
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
	if err := c.Engine.validate(c.WAL.Enabled, c.Replication.Role); err != nil {
		return err
	}
	if err := c.Network.validate(); err != nil {
		return err
	}
	if err := c.Logging.validate(); err != nil {
		return err
	}
	if c.Engine.Type == engine.TypeInMemory && c.WAL.DataDir == "" {
		return errors.New("wal dataDir cannot be empty")
	}
	if err := c.WAL.validate(); err != nil {
		return err
	}
	return c.Replication.validate(c.WAL.Enabled)
}

func (c *ServerNetworkConfig) validate() error {
	if c.Address == "" {
		return errors.New("network address cannot be empty")
	}
	if c.MaxConnections <= 0 {
		return errors.New("maxConnections must be positive")
	}
	if c.MaxMessageSizeKB <= 0 {
		return errors.New("maxMessageSize must be positive")
	}
	if c.MaxMessageSizeKB > math.MaxInt/1024 {
		return errors.New("maxMessageSize overflows bytes")
	}
	if c.IdleTimeout <= 0 {
		return errors.New("idleTimeout must be positive")
	}
	if c.ShutdownTimeout <= 0 {
		return errors.New("shutdownTimeout must be positive")
	}
	return nil
}

func (c *ServerLoggingConfig) validate() error {
	switch c.Level {
	case "debug", defaultLogLevel, "warn", "error":
		return nil
	default:
		return fmt.Errorf("unsupported logging level: %s", c.Level)
	}
}

func (c *ServerWALConfig) validate() error {
	if !c.Enabled {
		return nil
	}
	switch c.Sync {
	case wal.SyncAlways, wal.SyncEverySec, wal.SyncNo:
	default:
		return fmt.Errorf("unsupported wal sync policy: %s", c.Sync)
	}
	if c.SegmentSizeMB <= 0 {
		return errors.New("wal segmentSize must be positive")
	}
	if c.SegmentSizeMB > maxMB {
		return errors.New("wal segmentSize overflows bytes")
	}
	if c.SnapshotInterval <= 0 {
		return errors.New("wal snapshotInterval must be positive")
	}
	return nil
}

// validate checks engine settings. The tiered engine keeps its own durable segment store, so it is mutually exclusive
// with the WAL and with replication (which ships the WAL); enabling either alongside it is a configuration error.
func (c *ServerEngineConfig) validate(walEnabled bool, replicationRole string) error {
	switch c.Type {
	case engine.TypeInMemory:
		return nil
	case engine.TypeTiered:
	default:
		return fmt.Errorf("unsupported engine type: %s", c.Type)
	}

	if walEnabled {
		return errors.New("engine tiered cannot be combined with wal.enabled (it has its own durable store)")
	}
	if replicationRole != RoleStandalone {
		return errors.New("engine tiered does not support replication yet")
	}
	if c.DataDir == "" {
		return errors.New("engine data_dir cannot be empty")
	}
	return c.validateTieredStorage()
}

func (c *ServerEngineConfig) validateTieredStorage() error {
	if c.MaxMemoryMB <= 0 {
		return errors.New("engine max_memory must be positive")
	}
	if c.MaxMemoryMB > maxMB {
		return errors.New("engine max_memory overflows bytes")
	}
	if c.MaxStorageMB <= 0 {
		return errors.New("engine max_storage must be positive")
	}
	if c.MaxStorageMB > maxMB {
		return errors.New("engine max_storage overflows bytes")
	}
	if c.MaxMemoryMB > c.MaxStorageMB {
		return errors.New("engine max_memory must not exceed max_storage")
	}
	if c.SegmentSizeMB <= 0 {
		return errors.New("engine segment_size must be positive")
	}
	if c.SegmentSizeMB > maxMB {
		return errors.New("engine segment_size overflows bytes")
	}
	if c.CompactionThreshold <= 0 || c.CompactionThreshold > 1 {
		return errors.New("engine compaction_threshold must be in (0, 1]")
	}
	if c.CompactionInterval <= 0 {
		return errors.New("engine compaction_interval must be positive")
	}
	switch c.Sync {
	case wal.SyncAlways, wal.SyncEverySec, wal.SyncNo:
	default:
		return fmt.Errorf("unsupported engine sync policy: %s", c.Sync)
	}
	return nil
}

// validate checks replication settings. Replication requires WAL persistence, since the WAL is the replication stream.
func (r *ServerReplicationConfig) validate(walEnabled bool) error {
	switch r.Role {
	case RoleStandalone:
		return nil
	case RoleMaster:
		if r.ListenAddress == "" {
			return errors.New("replication master requires listen_address")
		}
	case RoleStandby:
		if r.MasterAddress == "" {
			return errors.New("replication standby requires master_address")
		}
		if r.ReconnectBackoff <= 0 {
			return errors.New("replication reconnect_backoff must be positive")
		}
	default:
		return fmt.Errorf("unsupported replication role: %s", r.Role)
	}
	if !walEnabled {
		return errors.New("replication requires wal.enabled")
	}
	return nil
}
