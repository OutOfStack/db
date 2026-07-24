package pool

import (
	"math/rand/v2"
	"sync"
	"time"
)

// ServerSelector is responsible for selecting servers from the pool
type ServerSelector interface {
	// Select returns the next server to use, or nil if no servers available.
	// It is equivalent to SelectRead and kept for backward compatibility.
	Select() *ServerConfig
	// SelectRead returns the next server for a read, following the strategy.
	SelectRead() *ServerConfig
	// SelectWrite returns the next master for a write, or nil if none available.
	// Standbys are never returned: writes must reach a master.
	SelectWrite() *ServerConfig
	// MarkFailed marks a server as failed
	MarkFailed(address string)
	// Reset resets the selector state
	Reset()
}

// MasterFirstSelector tries master servers first, then falls back to standbys
type MasterFirstSelector struct {
	mu             sync.RWMutex
	masters        []ServerConfig
	standbys       []ServerConfig
	failedServers  map[string]time.Time
	currentMaster  int
	currentStandby int
	failureTimeout time.Duration // Time after which failed servers are retried
}

// NewMasterFirstSelector creates a new master-first selector
func NewMasterFirstSelector(config *PoolConfig) *MasterFirstSelector {
	return &MasterFirstSelector{
		masters:        config.GetMasters(),
		standbys:       config.GetStandbys(),
		failedServers:  make(map[string]time.Time),
		failureTimeout: config.FailureTimeout,
	}
}

// Select picks the next available server (master first, then standby)
func (s *MasterFirstSelector) Select() *ServerConfig { return s.SelectRead() }

// SelectRead picks the next available server (master first, then standby)
func (s *MasterFirstSelector) SelectRead() *ServerConfig {
	s.mu.Lock()
	defer s.mu.Unlock()

	if server := s.selectMasterLocked(); server != nil {
		return server
	}

	// Fall back to standbys
	for i := range len(s.standbys) {
		idx := (s.currentStandby + i) % len(s.standbys)
		server := &s.standbys[idx]
		if !s.isFailed(server.Address) {
			s.currentStandby = (idx + 1) % len(s.standbys)
			return server
		}
	}

	return nil
}

// SelectWrite picks the next available master, or nil when none are available.
func (s *MasterFirstSelector) SelectWrite() *ServerConfig {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.selectMasterLocked()
}

func (s *MasterFirstSelector) selectMasterLocked() *ServerConfig {
	for i := range len(s.masters) {
		idx := (s.currentMaster + i) % len(s.masters)
		server := &s.masters[idx]
		if !s.isFailed(server.Address) {
			s.currentMaster = (idx + 1) % len(s.masters)
			return server
		}
	}
	return nil
}

// MarkFailed marks a server as failed
func (s *MasterFirstSelector) MarkFailed(address string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.failedServers[address] = time.Now()
}

// Reset resets the failed servers list
func (s *MasterFirstSelector) Reset() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.failedServers = make(map[string]time.Time)
	s.currentMaster = 0
	s.currentStandby = 0
}

func (s *MasterFirstSelector) isFailed(address string) bool {
	failedTime, failed := s.failedServers[address]
	if !failed {
		return false
	}

	// If failure timeout has passed, remove from failed list and allow retry
	if time.Since(failedTime) >= s.failureTimeout {
		delete(s.failedServers, address)
		return false
	}

	return true
}

// RoundRobinSelector rotates through all servers in order
type RoundRobinSelector struct {
	mu             sync.RWMutex
	servers        []ServerConfig
	masters        []ServerConfig
	current        int
	currentMaster  int
	failedServers  map[string]time.Time
	failureTimeout time.Duration
}

// NewRoundRobinSelector creates a new round-robin selector
func NewRoundRobinSelector(config *PoolConfig) *RoundRobinSelector {
	return &RoundRobinSelector{
		servers:        config.Servers,
		masters:        config.GetMasters(),
		failedServers:  make(map[string]time.Time),
		failureTimeout: config.FailureTimeout,
	}
}

// Select picks the next server in round-robin order
func (s *RoundRobinSelector) Select() *ServerConfig { return s.SelectRead() }

// SelectRead picks the next server in round-robin order across all servers
func (s *RoundRobinSelector) SelectRead() *ServerConfig {
	s.mu.Lock()
	defer s.mu.Unlock()
	return rotate(s.servers, &s.current, s.isFailed)
}

// SelectWrite picks the next master in round-robin order
func (s *RoundRobinSelector) SelectWrite() *ServerConfig {
	s.mu.Lock()
	defer s.mu.Unlock()
	return rotate(s.masters, &s.currentMaster, s.isFailed)
}

// rotate returns the next non-failed server from servers starting at *cursor,
// advancing the cursor past the chosen server.
func rotate(servers []ServerConfig, cursor *int, isFailed func(string) bool) *ServerConfig {
	for i := range servers {
		idx := (*cursor + i) % len(servers)
		server := &servers[idx]
		if !isFailed(server.Address) {
			*cursor = (idx + 1) % len(servers)
			return server
		}
	}
	return nil
}

// MarkFailed marks a server as failed
func (s *RoundRobinSelector) MarkFailed(address string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.failedServers[address] = time.Now()
}

// Reset resets the failed servers list
func (s *RoundRobinSelector) Reset() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.failedServers = make(map[string]time.Time)
	s.current = 0
}

func (s *RoundRobinSelector) isFailed(address string) bool {
	failedTime, failed := s.failedServers[address]
	if !failed {
		return false
	}

	// If failure timeout has passed, remove from failed list and allow retry
	if time.Since(failedTime) >= s.failureTimeout {
		delete(s.failedServers, address)
		return false
	}

	return true
}

// RandomSelector picks servers randomly
type RandomSelector struct {
	mu             sync.RWMutex
	servers        []ServerConfig
	masters        []ServerConfig
	failedServers  map[string]time.Time
	failureTimeout time.Duration
}

// NewRandomSelector creates a new random selector
func NewRandomSelector(config *PoolConfig) *RandomSelector {
	return &RandomSelector{
		servers:        config.Servers,
		masters:        config.GetMasters(),
		failedServers:  make(map[string]time.Time),
		failureTimeout: config.FailureTimeout,
	}
}

// Select picks a random available server
func (s *RandomSelector) Select() *ServerConfig { return s.SelectRead() }

// SelectRead picks a random available server across all servers
func (s *RandomSelector) SelectRead() *ServerConfig {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.pickRandom(s.servers)
}

// SelectWrite picks a random available master
func (s *RandomSelector) SelectWrite() *ServerConfig {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.pickRandom(s.masters)
}

func (s *RandomSelector) pickRandom(servers []ServerConfig) *ServerConfig {
	available := []int{}
	for i := range servers {
		if !s.isFailed(servers[i].Address) {
			available = append(available, i)
		}
	}
	if len(available) == 0 {
		return nil
	}
	//nolint:gosec // Non-cryptographic random is sufficient for server selection
	idx := available[rand.IntN(len(available))]
	return &servers[idx]
}

// MarkFailed marks a server as failed
func (s *RandomSelector) MarkFailed(address string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.failedServers[address] = time.Now()
}

// Reset resets the failed servers list
func (s *RandomSelector) Reset() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.failedServers = make(map[string]time.Time)
}

func (s *RandomSelector) isFailed(address string) bool {
	failedTime, failed := s.failedServers[address]
	if !failed {
		return false
	}

	// If failure timeout has passed, remove from failed list and allow retry
	if time.Since(failedTime) >= s.failureTimeout {
		delete(s.failedServers, address)
		return false
	}

	return true
}

// NewSelector creates a selector based on the strategy
//
//nolint:ireturn // Factory function intentionally returns interface
func NewSelector(config *PoolConfig) ServerSelector {
	switch config.SelectionStrategy {
	case StrategyMasterFirst:
		return NewMasterFirstSelector(config)
	case StrategyRoundRobin:
		return NewRoundRobinSelector(config)
	case StrategyRandom:
		return NewRandomSelector(config)
	default:
		return NewMasterFirstSelector(config)
	}
}
