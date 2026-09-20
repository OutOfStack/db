// Package client provides a Go client for the database server. It is the only package in this module intended for
// import by external programs:
//
//	import "github.com/OutOfStack/db/client"
//
//	c, err := client.New(client.WithAddress("127.0.0.1:3223"))
//	err = c.Set(ctx, "users", "name", "vlad")
//	val, err := c.Get(ctx, "users", "name")
//	err = c.Del(ctx, "users", "name")
//
// Values are typed. A value is sent as a literal, parsed server-side: 42 is an int, 42.5 a float, true a bool, [1,2] an
// array, {"a":1} a map, "42" and any other text a string. Set does not force a string — Set(ctx, t, k, "42") stores the
// int 42, and quoting it as "\"42\"" stores the string. Values come back in a human-readable form, and Type reports what
// a key holds.
//
// # Errors
//
// Three conditions have exported sentinels, because callers usually act on them rather than report them: ErrNotFound,
// ErrOutcomeUnknown, and ErrWrongType. Every other server failure is a *ServerError whose Code is a stable wire error
// code (see the Code constants) — branch on the code, never on the message text, which is not part of the
// compatibility promise.
//
// # Compatibility
//
// Within v1.x this package makes no incompatible change to its API, wire format, or error contract. See
// COMPATIBILITY.md in the repository for the full promise.
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

	// maxTableNameLen mirrors the server-side parser limit so invalid table names are rejected before reaching the wire
	maxTableNameLen = 128
)

// transport is the minimal connection interface the client needs. Satisfied by *network.TCPClient and *pool.Client
type transport interface {
	Send(ctx context.Context, cmd string, args []string) (protocol.Reply, error)
	Close() error
}

// Client is a client for the database server. These guarantees are frozen for v1.x.
//
// Concurrency: safe for concurrent use by any number of goroutines, including concurrently with Close. Commands on one
// connection are serialized, so throughput across goroutines is bounded by the number of pooled servers; a caller that
// needs more parallelism creates more clients.
//
// Contexts: every method takes a context that bounds the whole call — waiting for a free connection, dialling, writing,
// and reading the reply. Cancelling it interrupts a command already in flight rather than letting it run to the idle
// timeout.
//
// Cancellation after transmission: once a command has been written in full, no reply can prove whether it ran.
// Cancelling there returns ErrOutcomeUnknown for a mutation, and the error also matches the context error, so
// errors.Is(err, context.Canceled) and errors.Is(err, ErrOutcomeUnknown) both hold. The client never repeats such a
// command on its own; reads, and failures proven to have happened before any byte was sent, are retried transparently.
//
// Close: idempotent and terminal. Calling it more than once is safe and reports the first close's error contract; a
// closed client never reconnects, and later calls fail instead.
type Client struct {
	transport transport
}

// New creates a new Client configured by the given options. With WithServers, connections are pooled across the given
// servers, with reads retried on another server on failure; otherwise commands go to the address set by WithAddress
// (default 127.0.0.1:3223).
//
// No connection is made here — the first command connects, under its own context — so New reports configuration errors
// only, never an unreachable server.
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
	return &Client{transport: network.NewTCPClient(o.address, netOpts...)}, nil
}

// Set stores value under key in table. value is a typed literal parsed by the server, not an unconditional string:
// Set(ctx, t, k, "42") stores the int 42, Set(ctx, t, k, "[1,2]") an array, and Set(ctx, t, k, `"42"`) the string "42".
func (c *Client) Set(ctx context.Context, table, key, value string) error {
	if err := validateArgs(table, key, value); err != nil {
		return err
	}

	resp, err := c.send(ctx, "SET", []string{table, key, value})
	if err != nil {
		return err
	}
	return okReply(resp)
}

// Get returns the value stored under key in table. Returns ErrNotFound if the key does not exist.
func (c *Client) Get(ctx context.Context, table, key string) (string, error) {
	if err := validateArgs(table, key); err != nil {
		return "", err
	}

	resp, err := c.send(ctx, "GET", []string{table, key})
	if err != nil {
		return "", err
	}
	return textReply(resp)
}

// Del deletes key from table. Returns ErrNotFound if the key does not exist
func (c *Client) Del(ctx context.Context, table, key string) error {
	if err := validateArgs(table, key); err != nil {
		return err
	}

	resp, err := c.send(ctx, "DEL", []string{table, key})
	if err != nil {
		return err
	}
	if resp.Kind == protocol.ReplyNull {
		return ErrNotFound
	}
	return okReply(resp)
}

// Incr adds delta to the numeric value at key, creating it as 0 when the key is missing, and returns the new value.
// delta is a literal ("1", "-2", "0.5"); an empty delta increments by 1.
func (c *Client) Incr(ctx context.Context, table, key, delta string) (string, error) {
	if err := validateArgs(table, key); err != nil {
		return "", err
	}

	args := []string{table, key}
	if delta != "" {
		args = append(args, delta)
	}
	resp, err := c.send(ctx, "INCR", args)
	if err != nil {
		return "", err
	}
	return textReply(resp)
}

// Append pushes value onto the array at key, creating the array when the key is missing, and returns the new length.
func (c *Client) Append(ctx context.Context, table, key, value string) (int64, error) {
	if err := validateArgs(table, key, value); err != nil {
		return 0, err
	}

	resp, err := c.send(ctx, "APPEND", []string{table, key, value})
	if err != nil {
		return 0, err
	}
	if resp.Kind != protocol.ReplyInteger {
		return 0, errReply(resp)
	}
	return resp.Integer, nil
}

// HSet sets field of the map at key, creating the map when the key is missing.
func (c *Client) HSet(ctx context.Context, table, key, field, value string) error {
	if err := validateArgs(table, key, value); err != nil {
		return err
	}

	resp, err := c.send(ctx, "HSET", []string{table, key, field, value})
	if err != nil {
		return err
	}
	return okReply(resp)
}

// HGet returns one field of the map at key. Returns ErrNotFound if the key or the field does not exist.
func (c *Client) HGet(ctx context.Context, table, key, field string) (string, error) {
	if err := validateArgs(table, key); err != nil {
		return "", err
	}

	resp, err := c.send(ctx, "HGET", []string{table, key, field})
	if err != nil {
		return "", err
	}
	return textReply(resp)
}

// Type returns the type of the value at key: string, int, float, bool, array or map. Returns ErrNotFound if the key
// does not exist.
func (c *Client) Type(ctx context.Context, table, key string) (string, error) {
	if err := validateArgs(table, key); err != nil {
		return "", err
	}

	resp, err := c.send(ctx, "TYPE", []string{table, key})
	if err != nil {
		return "", err
	}
	return textReply(resp)
}

// textReply maps a value-returning reply to its text, with the same missing-key contract as Get.
func textReply(resp protocol.Reply) (string, error) {
	switch resp.Kind {
	case protocol.ReplyBulkString, protocol.ReplySimpleString:
		return resp.Value, nil
	case protocol.ReplyNull:
		return "", ErrNotFound
	default:
		return "", errReply(resp)
	}
}

// okReply maps a write acknowledgement to the client error contract: nil for +OK, an error otherwise.
func okReply(resp protocol.Reply) error {
	if resp.Kind == protocol.ReplySimpleString && resp.Value == respOK {
		return nil
	}
	return errReply(resp)
}

// errReply maps a reply already known not to be the expected kind to the shared error contract. A reply of an
// unexpected kind is a contract violation rather than a server-declared failure, so it carries no code.
func errReply(resp protocol.Reply) error {
	if resp.Kind == protocol.ReplyError {
		return &ServerError{Code: resp.Code, Msg: resp.Value}
	}
	return &ServerError{Msg: replyText(resp)}
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
	if resp.Kind != protocol.ReplyBulkString && resp.Kind != protocol.ReplySimpleString {
		return false, errReply(resp)
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

// Keys returns all keys in table in sorted order. A missing table returns an empty slice. The response is subject to
// the configured message-size limit.
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
	if resp.Kind != protocol.ReplyArray {
		return nil, errReply(resp)
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

// Raw sends a raw command line to the server and returns the reply as text. It gives access to commands that have no
// typed wrapper yet, and parses the line the way the CLI does: whitespace-separated tokens, with quoting and
// backslash escapes.
//
// The rendering is frozen for v1.x: a simple or bulk string is its own text, an integer its decimal digits, a null
// reply the text "not found", and an array its elements rendered the same way and joined with "\n" (nested arrays
// flatten into that join). An error reply is not rendered — it is returned as a *ServerError carrying the wire code,
// so a caller cannot mistake an error message for a value.
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
	if resp.Kind == protocol.ReplyError {
		return "", errReply(resp)
	}
	return replyText(resp), nil
}

// Close closes the client's connections and retires it: later calls fail rather than reconnecting. It is idempotent and
// terminal — safe to call more than once, and safe to call while other goroutines have commands in flight, which it
// interrupts rather than waiting for.
func (c *Client) Close() error {
	return c.transport.Close()
}

// send sends a command to the server and returns a typed response.
func (c *Client) send(ctx context.Context, cmd string, args []string) (protocol.Reply, error) {
	// nothing to connect or send for a context that is already done
	if err := ctx.Err(); err != nil {
		return protocol.Reply{}, err
	}

	// the error is returned unwrapped: callers match ErrOutcomeUnknown against it, and "failed to send" would misdescribe
	// a command that was sent
	return c.transport.Send(ctx, cmd, args)
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

// validateArgs checks command arguments that are still constrained by database semantics. RESP framing itself can carry
// whitespace, newlines, and NUL bytes.
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
		if quote == '\'' {
			if r == '\'' {
				quote = 0
			} else {
				current.WriteRune(r)
			}
			continue
		}

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
