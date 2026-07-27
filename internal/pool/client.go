package pool

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/OutOfStack/db/internal/network"
	"github.com/OutOfStack/db/internal/protocol"
)

// readOnlyReply is the error value a standby returns for a mutating command
// (wire "-ERR readonly", decoded with the "ERR " prefix stripped). It signals
// that the selected server is not actually a writable master.
const readOnlyReply = "readonly"

// syncedConnection wraps a TCPClient with a mutex to serialize Send() calls
type syncedConnection struct {
	mu     sync.Mutex
	client *network.TCPClient
}

// Send serializes Send() calls to prevent concurrent access corruption
func (sc *syncedConnection) Send(cmd string, args []string) (protocol.Reply, error) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	return sc.client.Send(cmd, args)
}

// Close closes the underlying connection
func (sc *syncedConnection) Close() error {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	return sc.client.Close()
}

// Client represents a pooled client that can connect to multiple servers
type Client struct {
	mu          sync.RWMutex
	config      *PoolConfig
	selector    ServerSelector
	connections map[string]*syncedConnection
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
		connections: make(map[string]*syncedConnection),
		options:     options,
	}, nil
}

// Send sends a command using the pool, with automatic failover. Writes (SET,
// DEL) route to a master; reads follow the configured strategy. A standby that
// replies "ERR readonly" to a write marks the routing stale, so the pool fails
// that server over and retries against another master.
func (c *Client) Send(cmd string, args []string) (protocol.Reply, error) {
	write := isWriteCommand(cmd)
	var lastErr error
	attempts := 0
	maxAttempts := c.config.MaxRetries + 1 // initial attempt + retries

	for attempts < maxAttempts {
		server := c.selectServer(write)
		if server == nil {
			return protocol.Reply{}, noServersError(write)
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
		resp, err := conn.Send(cmd, args)
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

		// A write that reached a read-only server means our master routing is
		// stale (the server was demoted); fail it over and retry elsewhere.
		if write && isReadOnlyReply(resp) {
			c.selector.MarkFailed(server.Address)
			lastErr = fmt.Errorf("server %s is read-only", server.Address)
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
		return protocol.Reply{}, fmt.Errorf("all servers failed after %d attempts: %w", attempts, lastErr)
	}
	return protocol.Reply{}, fmt.Errorf("all servers failed after %d attempts", attempts)
}

// selectServer picks a server for the command, resetting the selector once if
// all candidates are currently marked failed.
func (c *Client) selectServer(write bool) *ServerConfig {
	server := c.pick(write)
	if server == nil {
		c.selector.Reset()
		server = c.pick(write)
	}
	return server
}

func (c *Client) pick(write bool) *ServerConfig {
	if write {
		return c.selector.SelectWrite()
	}
	return c.selector.SelectRead()
}

func noServersError(write bool) error {
	if write {
		return errors.New("no master servers available in pool")
	}
	return errors.New("no servers available in pool")
}

// isWriteCommand reports whether cmd mutates state and must route to a master.
func isWriteCommand(cmd string) bool {
	switch strings.ToUpper(strings.TrimSpace(cmd)) {
	case "SET", "DEL":
		return true
	default:
		return false
	}
}

// isReadOnlyReply reports whether resp is a standby's "ERR readonly" response.
func isReadOnlyReply(resp protocol.Reply) bool {
	return resp.Kind == protocol.ReplyError && strings.EqualFold(resp.Value, readOnlyReply)
}

// getConnection gets or creates a connection to the specified address
func (c *Client) getConnection(address string) (*syncedConnection, error) {
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
	tcpClient, err := network.NewTCPClient(address, c.options...)
	if err != nil {
		return nil, fmt.Errorf("failed to create connection to %s: %w", address, err)
	}

	// Wrap with synchronized connection
	syncedConn := &syncedConnection{
		client: tcpClient,
	}

	c.connections[address] = syncedConn
	return syncedConn, nil
}

// removeConnection removes a connection from the pool
func (c *Client) removeConnection(address string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if conn, exists := c.connections[address]; exists {
		if err := conn.Close(); err != nil {
			// Log the error but continue with cleanup
			_ = err
		}
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

	c.connections = make(map[string]*syncedConnection)
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
