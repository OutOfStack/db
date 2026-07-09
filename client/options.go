package client

import "time"

// Role represents the role of a server in the pool
type Role string

const (
	// RoleMaster marks a server that accepts writes
	RoleMaster Role = "master"
	// RoleStandby marks a read-only standby server
	RoleStandby Role = "standby"
)

// Strategy defines how servers are selected from the pool
type Strategy string

const (
	// MasterFirst tries master servers first, falls back to standby
	MasterFirst Strategy = "master_first"
	// RoundRobin rotates through all servers
	RoundRobin Strategy = "round_robin"
	// Random picks a random server
	Random Strategy = "random"
)

// Server describes a single server in the pool
type Server struct {
	Address string
	Role    Role
}

// options holds the client configuration built from Option funcs
type options struct {
	address          string
	servers          []Server
	strategy         Strategy
	maxRetries       int
	retryDelay       time.Duration
	failureTimeout   time.Duration
	idleTimeout      time.Duration
	maxMessageSizeKB int
}

// defaultOptions returns options with sensible defaults
func defaultOptions() *options {
	return &options{
		address:          "127.0.0.1:3223",
		strategy:         MasterFirst,
		maxRetries:       3,
		retryDelay:       time.Second,
		failureTimeout:   30 * time.Second,
		idleTimeout:      time.Minute,
		maxMessageSizeKB: 4,
	}
}

// Option configures a Client
type Option func(*options)

// WithAddress sets the server address for single-server mode.
// Ignored when WithServers is also provided
func WithAddress(addr string) Option {
	return func(o *options) {
		o.address = addr
	}
}

// WithServers enables pool mode with the given servers.
// At least one server must have RoleMaster
func WithServers(servers ...Server) Option {
	return func(o *options) {
		o.servers = servers
	}
}

// WithStrategy sets the server selection strategy for pool mode
func WithStrategy(s Strategy) Option {
	return func(o *options) {
		o.strategy = s
	}
}

// WithRetries sets the number of retries and the delay between them for pool mode
func WithRetries(n int, delay time.Duration) Option {
	return func(o *options) {
		o.maxRetries = n
		o.retryDelay = delay
	}
}

// WithFailureTimeout sets the time after which failed servers are retried in pool mode
func WithFailureTimeout(d time.Duration) Option {
	return func(o *options) {
		if d > 0 {
			o.failureTimeout = d
		}
	}
}

// WithIdleTimeout sets the connection idle timeout
func WithIdleTimeout(d time.Duration) Option {
	return func(o *options) {
		if d > 0 {
			o.idleTimeout = d
		}
	}
}

// WithMaxMessageSize sets the maximum message size in kilobytes
func WithMaxMessageSize(kb int) Option {
	return func(o *options) {
		if kb > 0 {
			o.maxMessageSizeKB = kb
		}
	}
}
