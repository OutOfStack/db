package replication

import (
	"net"
	"time"
)

// settings holds the bounds and flags an Option can set on a master or standby.
type settings struct {
	maxConnections   int
	handshakeTimeout time.Duration
	idleTimeout      time.Duration
	// maxSnapshotBytes and maxSnapshotEntries cap a resync snapshot: the declared frame length is checked before a byte is
	// read, and the entry count while the verified snapshot is loaded into memory.
	maxSnapshotBytes   int64
	maxSnapshotEntries int
	// unverifiedHistory, when set, is why the master cannot vouch for the records it lost at recovery; it then refuses to
	// resume any standby incrementally (see Master).
	unverifiedHistory string
}

// Default snapshot bounds. They are sized for a preview replication deployment of the in-memory engine; an operator
// with a larger dataset raises replication.max_snapshot_size / max_snapshot_entries deliberately.
const (
	DefaultMaxSnapshotBytes   = 4 << 30
	DefaultMaxSnapshotEntries = 10_000_000
)

func defaultSettings() settings {
	return settings{
		maxConnections:     100,
		handshakeTimeout:   10 * time.Second,
		idleTimeout:        time.Minute,
		maxSnapshotBytes:   DefaultMaxSnapshotBytes,
		maxSnapshotEntries: DefaultMaxSnapshotEntries,
	}
}

// Option configures replication bounds. Non-positive values retain the defaults.
type Option func(*settings)

// WithMaxSnapshotBytes caps the declared length of a resync snapshot a standby will accept.
func WithMaxSnapshotBytes(n int64) Option {
	return func(s *settings) {
		if n > 0 {
			s.maxSnapshotBytes = n
		}
	}
}

// WithMaxSnapshotEntries caps how many entries of a resync snapshot a standby will load into memory.
func WithMaxSnapshotEntries(n int) Option {
	return func(s *settings) {
		if n > 0 {
			s.maxSnapshotEntries = n
		}
	}
}

// WithUnverifiedHistory marks a master as having recovered onto a history it cannot vouch for, giving the reason. Such
// a master serves only standbys that start from nothing (LSN 0) or that it can reseed from a snapshot; a standby asking
// to resume from an LSN it retains is refused, because that standby may hold records this master lost.
func WithUnverifiedHistory(reason string) Option {
	return func(s *settings) { s.unverifiedHistory = reason }
}

// WithMaxConnections caps simultaneous master connections, including incomplete handshakes.
func WithMaxConnections(n int) Option {
	return func(s *settings) {
		if n > 0 {
			s.maxConnections = n
		}
	}
}

// WithHandshakeTimeout bounds the entire initial handshake, including peers that trickle bytes.
func WithHandshakeTimeout(timeout time.Duration) Option {
	return func(s *settings) {
		if timeout > 0 {
			s.handshakeTimeout = timeout
		}
	}
}

// WithIdleTimeout bounds each socket read/write while streaming, including snapshot transfers.
// Standbys must allow more time than the master's heartbeat interval (at most one second).
func WithIdleTimeout(timeout time.Duration) Option {
	return func(s *settings) {
		if timeout > 0 {
			s.idleTimeout = timeout
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
