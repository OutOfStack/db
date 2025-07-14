package network

import (
	"errors"
	"fmt"
	"io"
	"net"
	"time"
)

// TCPClient represents a TCP client connection
type TCPClient struct {
	conn net.Conn

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
		conn: conn,
	}

	for _, option := range options {
		option(client)
	}

	if client.idleTimeout != 0 {
		if err = conn.SetDeadline(time.Now().Add(client.idleTimeout)); err != nil {
			return nil, fmt.Errorf("failed to set deadline: %w", err)
		}
	}

	return client, nil
}

// Send sends data to the server
func (tc *TCPClient) Send(data []byte) ([]byte, error) {
	if _, err := tc.conn.Write(data); err != nil {
		return nil, fmt.Errorf("failed to send data: %w", err)
	}

	resp := make([]byte, tc.bufferSize)
	n, err := tc.conn.Read(resp)
	if err != nil && !errors.Is(err, io.EOF) {
		return nil, fmt.Errorf("failed to read response: %w", err)
	} else if n == tc.bufferSize {
		return nil, errors.New("response too large")
	}

	return resp[:n], nil
}

// Close closes the TCP client connection
func (tc *TCPClient) Close() error {
	if tc.conn == nil {
		return nil
	}
	return tc.conn.Close()
}
