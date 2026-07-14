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
	"strconv"
	"strings"

	"github.com/OutOfStack/db/internal/network"
	"github.com/OutOfStack/db/internal/pool"
	"github.com/OutOfStack/db/internal/protocol"
)

const (
	respOK = "OK"

	// maxTableNameLen mirrors the server-side parser limit so invalid
	// table names are rejected before reaching the wire
	maxTableNameLen = 128
)

// transport is the minimal connection interface the client needs.
// Satisfied by *network.TCPClient and *pool.Client
type transport interface {
	Send(cmd string, args []string) (protocol.Reply, error)
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
		network.WithClientMaxMessageSize(o.maxMessageSizeKB * 1024),
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

	resp, err := c.send(ctx, "SET", []string{table, key, value})
	if err != nil {
		return err
	}
	switch resp.Kind {
	case protocol.ReplySimpleString:
		if resp.Value == respOK {
			return nil
		}
		return &ServerError{Msg: replyText(resp)}
	case protocol.ReplyError:
		return &ServerError{Msg: resp.Value}
	default:
		return &ServerError{Msg: replyText(resp)}
	}
}

// Get returns the value stored under key in table.
// Returns ErrNotFound if the key does not exist.
func (c *Client) Get(ctx context.Context, table, key string) (string, error) {
	if err := validateArgs(table, key); err != nil {
		return "", err
	}

	resp, err := c.send(ctx, "GET", []string{table, key})
	if err != nil {
		return "", err
	}
	switch resp.Kind {
	case protocol.ReplyBulkString, protocol.ReplySimpleString:
		return resp.Value, nil
	case protocol.ReplyNull:
		return "", ErrNotFound
	case protocol.ReplyError:
		return "", &ServerError{Msg: resp.Value}
	default:
		return "", &ServerError{Msg: replyText(resp)}
	}
}

// Del deletes key from table.
// Returns ErrNotFound if the key does not exist
func (c *Client) Del(ctx context.Context, table, key string) error {
	if err := validateArgs(table, key); err != nil {
		return err
	}

	resp, err := c.send(ctx, "DEL", []string{table, key})
	if err != nil {
		return err
	}
	switch resp.Kind {
	case protocol.ReplySimpleString:
		if resp.Value == respOK {
			return nil
		}
		return &ServerError{Msg: replyText(resp)}
	case protocol.ReplyNull:
		return ErrNotFound
	case protocol.ReplyError:
		return &ServerError{Msg: resp.Value}
	default:
		return &ServerError{Msg: replyText(resp)}
	}
}

// Tables returns all table names in sorted order.
func (c *Client) Tables(ctx context.Context) ([]string, error) {
	resp, err := c.send(ctx, "TABLES", nil)
	if err != nil {
		return nil, err
	}
	return stringArray(resp)
}

// TableExists reports whether table currently contains at least one key.
func (c *Client) TableExists(ctx context.Context, table string) (bool, error) {
	if err := validateArgs(table); err != nil {
		return false, err
	}

	resp, err := c.send(ctx, "EXISTS", []string{table})
	if err != nil {
		return false, err
	}
	if resp.Kind == protocol.ReplyError {
		return false, &ServerError{Msg: resp.Value}
	}
	if resp.Kind != protocol.ReplyBulkString && resp.Kind != protocol.ReplySimpleString {
		return false, &ServerError{Msg: replyText(resp)}
	}
	switch resp.Value {
	case "true":
		return true, nil
	case "false":
		return false, nil
	default:
		return false, &ServerError{Msg: "invalid EXISTS response: " + resp.Value}
	}
}

// Keys returns all keys in table in sorted order. A missing table returns an
// empty slice. The response is subject to the configured message-size limit.
func (c *Client) Keys(ctx context.Context, table string) ([]string, error) {
	if err := validateArgs(table); err != nil {
		return nil, err
	}

	resp, err := c.send(ctx, "KEYS", []string{table})
	if err != nil {
		return nil, err
	}
	return stringArray(resp)
}

func stringArray(resp protocol.Reply) ([]string, error) {
	if resp.Kind == protocol.ReplyError {
		return nil, &ServerError{Msg: resp.Value}
	}
	if resp.Kind != protocol.ReplyArray {
		return nil, &ServerError{Msg: replyText(resp)}
	}
	values := make([]string, 0, len(resp.Array))
	for _, item := range resp.Array {
		if item.Kind != protocol.ReplyBulkString && item.Kind != protocol.ReplySimpleString {
			return nil, &ServerError{Msg: "invalid list response"}
		}
		values = append(values, item.Value)
	}
	return values, nil
}

// Raw sends a raw command line to the server and returns the response text
// as is, without error mapping. It gives access to commands that have no
// typed wrapper yet.
func (c *Client) Raw(ctx context.Context, command string) (string, error) {
	parts, err := splitCommandLine(command)
	if err != nil {
		return "", err
	}
	if len(parts) == 0 {
		return "", errors.New("empty command")
	}
	resp, err := c.send(ctx, parts[0], parts[1:])
	if err != nil {
		return "", err
	}
	return replyText(resp), nil
}

// Close closes the client connection(s)
func (c *Client) Close() error {
	return c.transport.Close()
}

// send sends a command to the server and returns a typed response.
func (c *Client) send(ctx context.Context, cmd string, args []string) (protocol.Reply, error) {
	// the current transport cannot honor cancellation mid-call,
	// so check the context before sending
	if err := ctx.Err(); err != nil {
		return protocol.Reply{}, err
	}

	resp, err := c.transport.Send(cmd, args)
	if err != nil {
		return protocol.Reply{}, fmt.Errorf("failed to send command: %w", err)
	}
	return resp, nil
}

func replyText(reply protocol.Reply) string {
	switch reply.Kind {
	case protocol.ReplySimpleString, protocol.ReplyBulkString, protocol.ReplyError:
		return reply.Value
	case protocol.ReplyNull:
		return "not found"
	case protocol.ReplyInteger:
		return strconv.FormatInt(reply.Integer, 10)
	case protocol.ReplyArray:
		values := make([]string, 0, len(reply.Array))
		for _, item := range reply.Array {
			values = append(values, replyText(item))
		}
		return strings.Join(values, "\n")
	default:
		return ""
	}
}

// validateArgs checks command arguments that are still constrained by database
// semantics. RESP framing itself can carry whitespace, newlines, and NUL bytes.
func validateArgs(table string, args ...string) error {
	if table == "" {
		return errors.New("table cannot be empty")
	}
	if len(table) > maxTableNameLen {
		return fmt.Errorf("table name exceeds %d characters", maxTableNameLen)
	}
	if len(args) > 0 && args[0] == "" {
		return errors.New("key cannot be empty")
	}
	return nil
}

func splitCommandLine(command string) ([]string, error) {
	command = strings.TrimSpace(command)
	if command == "" {
		return nil, nil
	}

	var parts []string
	var current strings.Builder
	var quote rune
	escaped := false
	inToken := false

	for _, r := range command {
		if escaped {
			current.WriteRune(unescape(r))
			escaped = false
			inToken = true
			continue
		}

		if r == '\\' {
			escaped = true
			inToken = true
			continue
		}

		if quote != 0 {
			if r == quote {
				quote = 0
				continue
			}
			current.WriteRune(r)
			inToken = true
			continue
		}

		switch r {
		case '\'', '"':
			quote = r
			inToken = true
		case ' ', '\t', '\r', '\n':
			if inToken {
				parts = append(parts, current.String())
				current.Reset()
				inToken = false
			}
		default:
			current.WriteRune(r)
			inToken = true
		}
	}

	if escaped {
		return nil, errors.New("unfinished escape sequence")
	}
	if quote != 0 {
		return nil, errors.New("unterminated quoted string")
	}
	if inToken {
		parts = append(parts, current.String())
	}
	return parts, nil
}

func unescape(r rune) rune {
	switch r {
	case 'n':
		return '\n'
	case 'r':
		return '\r'
	case 't':
		return '\t'
	default:
		return r
	}
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
