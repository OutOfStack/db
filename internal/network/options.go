package network

import "time"

// TCPClientOption represents a functional option for configuring a TCPClient.
type TCPClientOption func(*TCPClient)

// WithClientIdleTimeout sets the idle timeout for a TCPClient.
func WithClientIdleTimeout(d time.Duration) TCPClientOption {
	return func(c *TCPClient) {
		c.idleTimeout = d
	}
}

// WithClientBufferSize sets the read/write buffer size for a TCPClient.
func WithClientBufferSize(size int) TCPClientOption {
	return func(c *TCPClient) {
		if size > 0 {
			c.bufferSize = size
		}
	}
}

// TCPServerOption represents a functional option for configuring a TCPServer.
type TCPServerOption func(*TCPServer)

// WithServerIdleTimeout sets the idle timeout for a TCPServer.
func WithServerIdleTimeout(d time.Duration) TCPServerOption {
	return func(s *TCPServer) {
		s.idleTimeout = d
	}
}

// WithServerBufferSize sets the read/write buffer size for a TCPServer.
func WithServerBufferSize(size int) TCPServerOption {
	return func(s *TCPServer) {
		if size > 0 {
			s.bufferSize = size
		}
	}
}

// WithServerMaxConnections sets the maximum number of concurrent connections for a TCPServer.
func WithServerMaxConnections(maxConnections int) TCPServerOption {
	return func(s *TCPServer) {
		if maxConnections > 0 {
			s.connectionSemaphore = make(chan struct{}, maxConnections)
		}
	}
}
