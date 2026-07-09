// Package client provides a Go client for the database server.
// It is the only package in this module intended for import by external programs:
//
//	import "github.com/OutOfStack/db/client"
//
//	c, err := client.New(client.WithAddress("127.0.0.1:3223"))
//	err = c.Set(ctx, "users", "name", "vlad")
//	val, err := c.Get(ctx, "users", "name")
//	err = c.Del(ctx, "users", "name")
package client

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"unicode"

	"github.com/OutOfStack/db/internal/network"
	"github.com/OutOfStack/db/internal/pool"
)

const (
	respOK       = "OK"
	respNotFound = "not found"
)

// transport is the minimal connection interface the client needs.
// Satisfied by *network.TCPClient and *pool.Client
type transport interface {
	Send(data []byte) ([]byte, error)
	Close() error
}

// Client is a client for the database server
type Client struct {
	transport transport
}

// New creates a new Client configured by the given options.
// With WithServers, connections are pooled across the given servers with
// automatic failover; otherwise a single connection is established to the
// address set by WithAddress (default 127.0.0.1:3223)
func New(opts ...Option) (*Client, error) {
	o := defaultOptions()
	for _, opt := range opts {
		opt(o)
	}

	netOpts := []network.TCPClientOption{
		network.WithClientIdleTimeout(o.idleTimeout),
		network.WithClientBufferSize(o.maxMessageSizeKB * 1024),
	}

	if len(o.servers) > 0 {
		poolCfg := &pool.PoolConfig{
			Enabled:           true,
			Servers:           toPoolServers(o.servers),
			SelectionStrategy: pool.SelectionStrategy(o.strategy),
			MaxRetries:        o.maxRetries,
			RetryDelay:        o.retryDelay,
			FailureTimeout:    o.failureTimeout,
		}
		poolClient, err := pool.NewClient(poolCfg, netOpts...)
		if err != nil {
			return nil, fmt.Errorf("failed to create pool client: %w", err)
		}
		return &Client{transport: poolClient}, nil
	}

	if o.address == "" {
		return nil, errors.New("address cannot be empty")
	}
	tcpClient, err := network.NewTCPClient(o.address, netOpts...)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to %s: %w", o.address, err)
	}
	return &Client{transport: tcpClient}, nil
}

// Set stores value under key in table
func (c *Client) Set(ctx context.Context, table, key, value string) error {
	if err := validateArgs(table, key, value); err != nil {
		return err
	}

	resp, err := c.send(ctx, "SET "+table+" "+key+" "+value)
	if err != nil {
		return err
	}
	if resp != respOK {
		return &ServerError{Msg: resp}
	}
	return nil
}

// Get returns the value stored under key in table.
// Returns ErrNotFound if the key does not exist
func (c *Client) Get(ctx context.Context, table, key string) (string, error) {
	if err := validateArgs(table, key); err != nil {
		return "", err
	}

	resp, err := c.send(ctx, "GET "+table+" "+key)
	if err != nil {
		return "", err
	}
	if resp == respNotFound {
		return "", ErrNotFound
	}
	return resp, nil
}

// Del deletes key from table.
// Returns ErrNotFound if the key does not exist
func (c *Client) Del(ctx context.Context, table, key string) error {
	if err := validateArgs(table, key); err != nil {
		return err
	}

	resp, err := c.send(ctx, "DEL "+table+" "+key)
	if err != nil {
		return err
	}
	switch resp {
	case respOK:
		return nil
	case respNotFound:
		return ErrNotFound
	default:
		return &ServerError{Msg: resp}
	}
}

// Raw sends a raw command line to the server and returns the response text
// as is, without error mapping. It gives access to commands that have no
// typed wrapper yet
func (c *Client) Raw(ctx context.Context, command string) (string, error) {
	if strings.TrimSpace(command) == "" {
		return "", errors.New("empty command")
	}
	return c.send(ctx, command)
}

// Close closes the client connection(s)
func (c *Client) Close() error {
	return c.transport.Close()
}

// send sends a command line to the server and returns the trimmed response
func (c *Client) send(ctx context.Context, command string) (string, error) {
	// the current transport cannot honor cancellation mid-call,
	// so check the context before sending
	if err := ctx.Err(); err != nil {
		return "", err
	}

	resp, err := c.transport.Send([]byte(command + "\n"))
	if err != nil {
		return "", fmt.Errorf("failed to send command: %w", err)
	}
	return strings.TrimSpace(string(resp)), nil
}

// validateArgs checks that command arguments can be carried by the
// whitespace-delimited text protocol
func validateArgs(args ...string) error {
	for _, arg := range args {
		if arg == "" {
			return errors.New("argument cannot be empty")
		}
		if strings.ContainsFunc(arg, unicode.IsSpace) {
			return fmt.Errorf("argument %q cannot contain whitespace", arg)
		}
	}
	return nil
}

// toPoolServers converts public Server values to pool config entries
func toPoolServers(servers []Server) []pool.ServerConfig {
	out := make([]pool.ServerConfig, 0, len(servers))
	for _, s := range servers {
		out = append(out, pool.ServerConfig{
			Address: s.Address,
			Role:    pool.ServerRole(s.Role),
		})
	}
	return out
}
