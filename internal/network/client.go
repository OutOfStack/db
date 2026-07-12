package network

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"strings"
	"time"

	"github.com/OutOfStack/db/internal/protocol"
)

// TCPClient represents a TCP client connection
type TCPClient struct {
	conn    net.Conn
	reader  *bufio.Reader
	address string

	idleTimeout    time.Duration
	maxMessageSize int
}

// NewTCPClient creates a new TCP client connection
func NewTCPClient(address string, options ...TCPClientOption) (*TCPClient, error) {
	dialer := &net.Dialer{
		Timeout: 10 * time.Second,
	}
	conn, err := dialer.DialContext(context.Background(), "tcp", address)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to %s: %w", address, err)
	}

	client := &TCPClient{
		conn:           conn,
		reader:         bufio.NewReader(conn),
		address:        address,
		maxMessageSize: defaultMaxMessageSize,
		idleTimeout:    defaultTimeout,
	}

	for _, option := range options {
		option(client)
	}

	return client, nil
}

// Send sends a command to the server with automatic reconnection on connection failures
func (tc *TCPClient) Send(cmd string, args []string) (protocol.Reply, error) {
	return tc.sendWithRetry(cmd, args, true)
}

// sendWithRetry attempts to send data with optional retry on connection failure
func (tc *TCPClient) sendWithRetry(cmd string, args []string, allowRetry bool) (protocol.Reply, error) {
	// set write deadline
	if err := tc.conn.SetWriteDeadline(time.Now().Add(tc.idleTimeout)); err != nil {
		return protocol.Reply{}, fmt.Errorf("failed to set write deadline: %w", err)
	}

	// send request
	if err := protocol.WriteCommand(tc.conn, cmd, args); err != nil {
		if allowRetry && tc.isConnectionError(err) {
			if rErr := tc.reconnect(); rErr != nil {
				return protocol.Reply{}, fmt.Errorf("failed to reconnect: %w", rErr)
			}
			return tc.sendWithRetry(cmd, args, false)
		}
		return protocol.Reply{}, fmt.Errorf("failed to send data: %w", err)
	}

	// set read deadline
	if err := tc.conn.SetReadDeadline(time.Now().Add(tc.idleTimeout)); err != nil {
		return protocol.Reply{}, fmt.Errorf("failed to set read deadline: %w", err)
	}

	// read response
	resp, err := protocol.ReadReply(tc.reader, tc.maxMessageSize)
	if err != nil {
		return tc.handleReadError(cmd, args, err, allowRetry)
	}

	return resp, nil
}

// handleReadError recovers from a failed reply read: connection errors are
// retried once on a fresh connection; decode errors drop the connection
// because unread reply bytes may remain on the wire and would otherwise be
// read as the next command's reply
func (tc *TCPClient) handleReadError(cmd string, args []string, err error, allowRetry bool) (protocol.Reply, error) {
	if !tc.isConnectionError(err) {
		// reconnect eagerly; if that fails the connection is still closed
		// and the next Send re-dials via its own retry path
		_ = tc.reconnect()
		return protocol.Reply{}, fmt.Errorf("failed to read response: %w", err)
	}
	if allowRetry {
		if rErr := tc.reconnect(); rErr != nil {
			return protocol.Reply{}, fmt.Errorf("failed to reconnect: %w", rErr)
		}
		return tc.sendWithRetry(cmd, args, false)
	}
	return protocol.Reply{}, fmt.Errorf("failed to read response: %w", err)
}

// isConnectionError checks if the error indicates a broken connection
func (tc *TCPClient) isConnectionError(err error) bool {
	if err == nil {
		return false
	}

	// check for EOF (connection closed by server)
	if errors.Is(err, io.EOF) {
		return true
	}

	// check for closed connection
	if errors.Is(err, net.ErrClosed) {
		return true
	}

	// check for timeout errors
	if netErr, ok := errors.AsType[net.Error](err); ok && netErr.Timeout() {
		return true
	}

	// check for OpError with specific operations
	if opErr, ok := errors.AsType[*net.OpError](err); ok {
		// check for connection-related operations
		switch opErr.Op {
		case "read", "write", "dial":
			return true
		}
	}

	// fallback to string matching for compatibility
	errStr := err.Error()
	return strings.Contains(errStr, "broken pipe") ||
		strings.Contains(errStr, "connection reset") ||
		strings.Contains(errStr, "connection refused")
}

// reconnect attempts to establish a new connection to the server
func (tc *TCPClient) reconnect() error {
	// close existing connection
	if err := tc.Close(); err != nil {
		log.Printf("Failed to close existing connection during reconnect: %v", err)
	}

	// establish new connection
	dialer := &net.Dialer{
		Timeout: 10 * time.Second,
	}
	conn, err := dialer.DialContext(context.Background(), "tcp", tc.address)
	if err != nil {
		return fmt.Errorf("failed to reconnect to %s: %w", tc.address, err)
	}

	tc.conn = conn
	tc.reader = bufio.NewReader(conn)
	return nil
}

// Close closes the TCP client connection
func (tc *TCPClient) Close() error {
	if tc.conn == nil {
		return nil
	}
	return tc.conn.Close()
}
