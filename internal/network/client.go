package network

import (
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"strings"
	"time"
)

// TCPClient represents a TCP client connection
type TCPClient struct {
	conn    net.Conn
	address string

	idleTimeout time.Duration
	bufferSize  int
}

// NewTCPClient creates a new TCP client connection
func NewTCPClient(address string, options ...TCPClientOption) (*TCPClient, error) {
	conn, err := net.Dial("tcp", address)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to %s: %w", address, err)
	}

	client := &TCPClient{
		conn:        conn,
		address:     address,
		bufferSize:  defaultBufferSize,
		idleTimeout: defaultTimeout,
	}

	for _, option := range options {
		option(client)
	}

	return client, nil
}

// Send sends data to the server with automatic reconnection on connection failures
func (tc *TCPClient) Send(data []byte) ([]byte, error) {
	return tc.sendWithRetry(data, true)
}

// sendWithRetry attempts to send data with optional retry on connection failure
func (tc *TCPClient) sendWithRetry(data []byte, allowRetry bool) ([]byte, error) {
	// set write deadline
	if err := tc.conn.SetWriteDeadline(time.Now().Add(tc.idleTimeout)); err != nil {
		return nil, fmt.Errorf("failed to set write deadline: %w", err)
	}

	// send request
	if _, err := tc.conn.Write(data); err != nil {
		if allowRetry && tc.isConnectionError(err) {
			if rErr := tc.reconnect(); rErr != nil {
				return nil, fmt.Errorf("failed to reconnect: %w", rErr)
			}
			return tc.sendWithRetry(data, false)
		}
		return nil, fmt.Errorf("failed to send data: %w", err)
	}

	// set read deadline
	if err := tc.conn.SetReadDeadline(time.Now().Add(tc.idleTimeout)); err != nil {
		return nil, fmt.Errorf("failed to set read deadline: %w", err)
	}

	// read response
	resp := make([]byte, tc.bufferSize)
	n, err := tc.conn.Read(resp)
	if err != nil {
		if allowRetry && tc.isConnectionError(err) {
			if rErr := tc.reconnect(); rErr != nil {
				return nil, fmt.Errorf("failed to reconnect: %w", rErr)
			}
			return tc.sendWithRetry(data, false)
		}
		return nil, fmt.Errorf("failed to read response: %w", err)
	} else if n == tc.bufferSize {
		return nil, errors.New("response too large")
	}

	return resp[:n], nil
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
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return true
	}

	// check for OpError with specific operations
	var opErr *net.OpError
	if errors.As(err, &opErr) {
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
	conn, err := net.Dial("tcp", tc.address)
	if err != nil {
		return fmt.Errorf("failed to reconnect to %s: %w", tc.address, err)
	}

	tc.conn = conn
	return nil
}

// Close closes the TCP client connection
func (tc *TCPClient) Close() error {
	if tc.conn == nil {
		return nil
	}
	return tc.conn.Close()
}
