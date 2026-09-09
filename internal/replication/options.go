package replication

import (
	"net"
	"time"
)

type peerLimits struct {
	maxConnections   int
	handshakeTimeout time.Duration
	idleTimeout      time.Duration
}

func defaultPeerLimits() peerLimits {
	return peerLimits{maxConnections: 100, handshakeTimeout: 10 * time.Second, idleTimeout: time.Minute}
}

// Option configures replication connection bounds. Non-positive values retain the defaults.
type Option func(*peerLimits)

// WithMaxConnections caps simultaneous master connections, including incomplete handshakes.
func WithMaxConnections(n int) Option {
	return func(l *peerLimits) {
		if n > 0 {
			l.maxConnections = n
		}
	}
}

// WithHandshakeTimeout bounds the entire initial handshake, including peers that trickle bytes.
func WithHandshakeTimeout(timeout time.Duration) Option {
	return func(l *peerLimits) {
		if timeout > 0 {
			l.handshakeTimeout = timeout
		}
	}
}

// WithIdleTimeout bounds each socket read/write while streaming, including snapshot transfers.
// Standbys must allow more time than the master's heartbeat interval (at most one second).
func WithIdleTimeout(timeout time.Duration) Option {
	return func(l *peerLimits) {
		if timeout > 0 {
			l.idleTimeout = timeout
		}
	}
}

// deadlineConn renews deadlines on socket I/O so large snapshots can keep progressing without an overall time limit.
type deadlineConn struct {
	net.Conn
	timeout time.Duration
}

func (c deadlineConn) Read(p []byte) (int, error) {
	if err := c.SetReadDeadline(time.Now().Add(c.timeout)); err != nil {
		return 0, err
	}
	return c.Conn.Read(p)
}

func (c deadlineConn) Write(p []byte) (int, error) {
	if err := c.SetWriteDeadline(time.Now().Add(c.timeout)); err != nil {
		return 0, err
	}
	return c.Conn.Write(p)
}
