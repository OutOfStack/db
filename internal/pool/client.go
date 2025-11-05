package pool

import (
	"fmt"
	"sync"
	"time"

	"github.com/OutOfStack/db/internal/network"
)

// Client represents a pooled client that can connect to multiple servers
type Client struct {
	mu          sync.RWMutex
	config      *PoolConfig
	selector    ServerSelector
	connections map[string]*network.TCPClient
	options     []network.TCPClientOption
}

// NewClient creates a new pooled client
func NewClient(config *PoolConfig, options ...network.TCPClientOption) (*Client, error) {
	if err := config.Validate(); err != nil {
		return nil, fmt.Errorf("invalid pool config: %w", err)
	}

	return &Client{
		config:      config,
		selector:    NewSelector(config),
		connections: make(map[string]*network.TCPClient),
		options:     options,
	}, nil
}

// Send sends data using the pool, with automatic failover
func (c *Client) Send(data []byte) ([]byte, error) {
	var lastErr error
	attempts := 0
	maxAttempts := c.config.MaxRetries + 1 // initial attempt + retries

	for attempts < maxAttempts {
		// Select a server
		server := c.selector.Select()
		if server == nil {
			// No servers available, reset and try one more time
			c.selector.Reset()
			server = c.selector.Select()
			if server == nil {
				return nil, fmt.Errorf("no servers available in pool")
			}
		}

		// Get or create connection
		conn, err := c.getConnection(server.Address)
		if err != nil {
			c.selector.MarkFailed(server.Address)
			lastErr = err
			attempts++
			if attempts < maxAttempts {
				time.Sleep(c.config.RetryDelay)
			}
			continue
		}

		// Try to send
		resp, err := conn.Send(data)
		if err != nil {
			// Connection failed, mark server as failed and retry
			c.selector.MarkFailed(server.Address)
			c.removeConnection(server.Address)
			lastErr = fmt.Errorf("failed to send to %s: %w", server.Address, err)
			attempts++
			if attempts < maxAttempts {
				time.Sleep(c.config.RetryDelay)
			}
			continue
		}

		// Success!
		return resp, nil
	}

	if lastErr != nil {
		return nil, fmt.Errorf("all servers failed after %d attempts: %w", attempts, lastErr)
	}
	return nil, fmt.Errorf("all servers failed after %d attempts", attempts)
}

// getConnection gets or creates a connection to the specified address
func (c *Client) getConnection(address string) (*network.TCPClient, error) {
	c.mu.RLock()
	conn, exists := c.connections[address]
	c.mu.RUnlock()

	if exists {
		return conn, nil
	}

	// Create new connection
	c.mu.Lock()
	defer c.mu.Unlock()

	// Double-check after acquiring write lock
	if conn, exists := c.connections[address]; exists {
		return conn, nil
	}

	// Create new TCP client
	newConn, err := network.NewTCPClient(address, c.options...)
	if err != nil {
		return nil, fmt.Errorf("failed to create connection to %s: %w", address, err)
	}

	c.connections[address] = newConn
	return newConn, nil
}

// removeConnection removes a connection from the pool
func (c *Client) removeConnection(address string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if conn, exists := c.connections[address]; exists {
		conn.Close()
		delete(c.connections, address)
	}
}

// Close closes all connections in the pool
func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	var lastErr error
	for address, conn := range c.connections {
		if err := conn.Close(); err != nil {
			lastErr = fmt.Errorf("failed to close connection to %s: %w", address, err)
		}
	}

	c.connections = make(map[string]*network.TCPClient)
	return lastErr
}

// Reset resets the pool selector state
func (c *Client) Reset() {
	c.selector.Reset()
}

// GetActiveServers returns the addresses of servers with active connections
func (c *Client) GetActiveServers() []string {
	c.mu.RLock()
	defer c.mu.RUnlock()

	servers := make([]string, 0, len(c.connections))
	for addr := range c.connections {
		servers = append(servers, addr)
	}
	return servers
}
