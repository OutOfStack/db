package pool

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/OutOfStack/db/internal/network"
	"github.com/OutOfStack/db/internal/parser"
	"github.com/OutOfStack/db/internal/protocol"
)

// readOnlyReply is the error value a standby returns for a mutating command (wire "-ERR readonly", decoded with the
// "ERR " prefix stripped). It signals that the selected server is not actually a writable master.
const readOnlyReply = "readonly"

// Client represents a pooled client that can connect to multiple servers
type Client struct {
	mu          sync.RWMutex
	config      *PoolConfig
	selector    ServerSelector
	connections map[string]*network.TCPClient
	options     []network.TCPClientOption
	closed      bool
	// done is closed by Close, so a call parked in a retry delay stops waiting instead of sleeping it out
	done chan struct{}
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
		done:        make(chan struct{}),
	}, nil
}

// Send sends a command using the pool. Mutating commands route to the master; reads follow the configured strategy and
// retry on another server after a failure. A server that replies "ERR readonly" to a write marks the routing stale, so
// the pool treats it as failed and retries. Admin commands are refused client-side: they target one specific node, and
// the pool cannot promise which server a routed command reaches.
//
// Whether a failure may be retried is decided by the transport, not here: an error carrying network.ErrOutcomeUnknown
// means the command may already have run, so it is never sent to another server. Every other failure provably did not
// execute, which is what makes trying the next server safe.
func (c *Client) Send(ctx context.Context, cmd string, args []string) (protocol.Reply, error) {
	if parser.IsAdmin(cmd) {
		return protocol.Reply{}, fmt.Errorf("admin command %s cannot be sent through a pool; connect to the target server directly", cmd)
	}
	write := parser.IsWrite(cmd)
	var lastErr error
	maxAttempts := c.config.MaxRetries + 1 // initial attempt + retries

	attempts := 0
	for ; attempts < maxAttempts; attempts++ {
		if attempts > 0 {
			if err := c.wait(ctx, c.config.RetryDelay); err != nil {
				return protocol.Reply{}, err
			}
		}

		server := c.selectServer(write)
		if server == nil {
			return protocol.Reply{}, noServersError(write)
		}

		conn, err := c.getConnection(server.Address)
		if err != nil {
			return protocol.Reply{}, err
		}

		resp, err := conn.Send(ctx, cmd, args)
		if err != nil {
			c.selector.MarkFailed(server.Address)
			lastErr = fmt.Errorf("failed to send to %s: %w", server.Address, err)
			if errors.Is(err, network.ErrOutcomeUnknown) {
				// the command may already have taken effect on this server; running it anywhere else risks applying it twice
				return protocol.Reply{}, lastErr
			}
			continue
		}

		// A write that reached a read-only server means our master routing is stale (the server was demoted); mark it
		// failed and retry. A pool holds one master, so the retry revisits it: the attempts drain and the caller gets the
		// read-only error rather than a false success.
		if write && isReadOnlyReply(resp) {
			c.selector.MarkFailed(server.Address)
			lastErr = fmt.Errorf("server %s is read-only", server.Address)
			continue
		}

		return resp, nil
	}

	if lastErr != nil {
		return protocol.Reply{}, fmt.Errorf("all servers failed after %d attempts: %w", attempts, lastErr)
	}
	return protocol.Reply{}, fmt.Errorf("all servers failed after %d attempts", attempts)
}

// wait pauses between attempts, giving up as soon as the caller cancels or the pool is closed. Closing has to reach it:
// a retry delay is configurable and can be far longer than a caller expects a closed pool to keep working.
func (c *Client) wait(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-c.done:
		return net.ErrClosed
	case <-timer.C:
		return nil
	}
}

// selectServer picks a server for the command, resetting the selector once if all candidates are currently marked
// failed.
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

// isReadOnlyReply reports whether resp is a standby's "ERR readonly" response.
func isReadOnlyReply(resp protocol.Reply) bool {
	return resp.Kind == protocol.ReplyError && strings.EqualFold(resp.Value, readOnlyReply)
}

// getConnection returns the client for address, creating it on first use. TCPClient connects lazily and serializes its
// own commands, so this neither performs I/O nor needs a wrapper of its own.
//
// Connections are never evicted on failure. A TCPClient drops its socket on any I/O error and redials on the next
// command, so the entry is self-healing, and evicting it would close a connection that other goroutines may be sending
// on — turning one server's hiccup into an unknown outcome for every command in flight against it. Keeping the server
// out of rotation is MarkFailed's job.
func (c *Client) getConnection(address string) (*network.TCPClient, error) {
	c.mu.RLock()
	conn, exists := c.connections[address]
	c.mu.RUnlock()

	if exists {
		return conn, nil
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return nil, net.ErrClosed
	}

	// Double-check after acquiring write lock
	if conn, exists = c.connections[address]; exists {
		return conn, nil
	}

	conn = network.NewTCPClient(address, c.options...)
	c.connections[address] = conn
	return conn, nil
}

// Close closes all connections and retires the pool: later commands fail with net.ErrClosed rather than reconnecting.
// It is safe to call more than once.
func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if !c.closed {
		c.closed = true
		close(c.done)
	}

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

// GetActiveServers returns the addresses the pool has sent to. Connections are lazy and self-healing, so an address
// here has been used at some point, not necessarily an open socket right now.
func (c *Client) GetActiveServers() []string {
	c.mu.RLock()
	defer c.mu.RUnlock()

	servers := make([]string, 0, len(c.connections))
	for addr := range c.connections {
		servers = append(servers, addr)
	}
	return servers
}
