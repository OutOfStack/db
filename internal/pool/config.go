package pool

import (
	"errors"
	"time"
)

// ServerRole represents the role of a server in the pool
type ServerRole string

const (
	RoleMaster  ServerRole = "master"
	RoleStandby ServerRole = "standby"
)

// SelectionStrategy defines how servers are selected from the pool
type SelectionStrategy string

const (
	StrategyMasterFirst SelectionStrategy = "master_first" // Try master first, fallback to standby
	StrategyRoundRobin  SelectionStrategy = "round_robin"  // Rotate through all servers
	StrategyRandom      SelectionStrategy = "random"       // Pick random server
)

// ServerConfig represents a single server in the pool
type ServerConfig struct {
	Address string     `yaml:"address"`
	Role    ServerRole `yaml:"role"`
}

// PoolConfig represents the configuration for a connection pool
type PoolConfig struct {
	Enabled           bool              `yaml:"enabled"`
	Servers           []ServerConfig    `yaml:"servers"`
	SelectionStrategy SelectionStrategy `yaml:"selection_strategy"`
	MaxRetries        int               `yaml:"max_retries"`
	RetryDelay        time.Duration     `yaml:"retry_delay"`
	HealthCheckPeriod time.Duration     `yaml:"health_check_period"`
}

// DefaultPoolConfig returns a PoolConfig with sensible defaults
func DefaultPoolConfig() *PoolConfig {
	return &PoolConfig{
		Enabled:           false,
		Servers:           []ServerConfig{},
		SelectionStrategy: StrategyMasterFirst,
		MaxRetries:        3,
		RetryDelay:        time.Second,
		HealthCheckPeriod: 10 * time.Second,
	}
}

// Validate checks if the pool configuration is valid
func (p *PoolConfig) Validate() error {
	if !p.Enabled {
		return nil
	}

	if len(p.Servers) == 0 {
		return errors.New("pool enabled but no servers configured")
	}

	masterCount := 0
	for i, server := range p.Servers {
		if server.Address == "" {
			return errors.New("server address cannot be empty")
		}
		if server.Role == "" {
			return errors.New("server role cannot be empty")
		}
		if server.Role != RoleMaster && server.Role != RoleStandby {
			return errors.New("server role must be 'master' or 'standby'")
		}
		if server.Role == RoleMaster {
			masterCount++
		}

		// Check for duplicate addresses
		for j := i + 1; j < len(p.Servers); j++ {
			if server.Address == p.Servers[j].Address {
				return errors.New("duplicate server address: " + server.Address)
			}
		}
	}

	if masterCount == 0 {
		return errors.New("at least one master server is required")
	}

	if p.SelectionStrategy != StrategyMasterFirst &&
		p.SelectionStrategy != StrategyRoundRobin &&
		p.SelectionStrategy != StrategyRandom {
		return errors.New("invalid selection strategy")
	}

	if p.MaxRetries < 0 {
		return errors.New("max_retries cannot be negative")
	}

	if p.RetryDelay < 0 {
		return errors.New("retry_delay cannot be negative")
	}

	return nil
}

// GetMasters returns all servers with master role
func (p *PoolConfig) GetMasters() []ServerConfig {
	masters := []ServerConfig{}
	for _, server := range p.Servers {
		if server.Role == RoleMaster {
			masters = append(masters, server)
		}
	}
	return masters
}

// GetStandbys returns all servers with standby role
func (p *PoolConfig) GetStandbys() []ServerConfig {
	standbys := []ServerConfig{}
	for _, server := range p.Servers {
		if server.Role == RoleStandby {
			standbys = append(standbys, server)
		}
	}
	return standbys
}
