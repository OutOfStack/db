package replication

import (
	"bufio"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"sync/atomic"
	"time"

	"github.com/OutOfStack/db/internal/engine"
	"github.com/OutOfStack/db/internal/protocol"
	"github.com/OutOfStack/db/internal/wal"
)

// Applier persists and applies replicated records. It is satisfied by
// *storage.Storage, which routes replicated writes through the same lock as
// snapshots so a snapshot never captures a state ahead of the engine.
type Applier interface {
	// ApplyReplicated persists a record and applies it to the engine.
	ApplyReplicated(ctx context.Context, record wal.Record) error
	// ResetToSnapshot replaces all state with a resync snapshot at lsn.
	ResetToSnapshot(ctx context.Context, dir string, lsn uint64, entries []engine.Entry) error
}

// defaultReconnectBackoff is the pause between replication reconnect attempts.
const defaultReconnectBackoff = time.Second

// maxSnapshotRecordSize bounds a single decoded SET command inside a snapshot
// stream, mirroring the WAL's per-record limit. maxSnapshotBytes bounds the whole
// snapshot blob so a malformed length cannot drive an unbounded read.
const (
	maxSnapshotRecordSize = 64 << 20
	maxSnapshotBytes      = 1 << 40
)

// Standby connects to a master, persists the streamed WAL to its own log, and
// applies it to its engine in order. It reconnects with backoff and tracks the
// applied LSN and lag so a promoted standby has a complete, contiguous log.
type Standby struct {
	masterAddr string
	applier    Applier
	dir        string
	logger     *slog.Logger
	backoff    time.Duration
	dialer     *net.Dialer

	appliedLSN atomic.Uint64
	masterLSN  atomic.Uint64
	connected  atomic.Bool

	cancel context.CancelFunc
	done   chan struct{}
}

// NewStandby creates a standby that replicates from masterAddr into applier (the
// server's storage). dir is the WAL/snapshot directory. appliedLSN is the last
// LSN already durable, used as the resume point in the first handshake.
func NewStandby(
	masterAddr string,
	applier Applier,
	dir string,
	appliedLSN uint64,
	backoff time.Duration,
	logger *slog.Logger,
) *Standby {
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	if backoff <= 0 {
		backoff = defaultReconnectBackoff
	}
	s := &Standby{
		masterAddr: masterAddr,
		applier:    applier,
		dir:        dir,
		logger:     logger,
		backoff:    backoff,
		dialer:     &net.Dialer{Timeout: 10 * time.Second},
		done:       make(chan struct{}),
	}
	s.appliedLSN.Store(appliedLSN)
	s.masterLSN.Store(appliedLSN)
	return s
}

// Start begins the replication loop in the background.
func (s *Standby) Start(ctx context.Context) {
	ctx, cancel := context.WithCancel(ctx)
	s.cancel = cancel
	go s.run(ctx)
}

// Stop ends replication and waits for the loop to exit.
func (s *Standby) Stop() {
	if s.cancel != nil {
		s.cancel()
	}
	<-s.done
}

// AppliedLSN returns the highest LSN this standby has persisted and applied.
func (s *Standby) AppliedLSN() uint64 { return s.appliedLSN.Load() }

// Lag returns how many LSNs the standby trails the master by, based on the
// latest record or heartbeat seen. It is a best-effort, eventually-consistent
// estimate given asynchronous replication.
func (s *Standby) Lag() uint64 {
	master := s.masterLSN.Load()
	applied := s.appliedLSN.Load()
	if master <= applied {
		return 0
	}
	return master - applied
}

// Connected reports whether the standby currently has a live master stream.
func (s *Standby) Connected() bool { return s.connected.Load() }

func (s *Standby) run(ctx context.Context) {
	defer close(s.done)
	for {
		if ctx.Err() != nil {
			return
		}
		err := s.replicateOnce(ctx)
		if ctx.Err() != nil {
			return
		}
		if err != nil {
			s.logger.Warn("Replication disconnected, retrying", "error", err, "backoff", s.backoff)
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(s.backoff):
		}
	}
}

func (s *Standby) replicateOnce(ctx context.Context) error {
	conn, err := s.dialer.DialContext(ctx, "tcp", s.masterAddr)
	if err != nil {
		return fmt.Errorf("dial master %s: %w", s.masterAddr, err)
	}
	defer func() { _ = conn.Close() }()
	stop := context.AfterFunc(ctx, func() { _ = conn.Close() })
	defer stop()

	if err = writeHandshake(conn, s.appliedLSN.Load()); err != nil {
		return fmt.Errorf("send handshake: %w", err)
	}
	s.connected.Store(true)
	defer s.connected.Store(false)
	s.logger.Info("Replicating from master", "master", s.masterAddr, "from_lsn", s.appliedLSN.Load())

	reader := bufio.NewReader(conn)
	for {
		if err = s.readFrame(ctx, reader); err != nil {
			return err
		}
	}
}

func (s *Standby) readFrame(ctx context.Context, reader *bufio.Reader) error {
	frameType, err := reader.ReadByte()
	if err != nil {
		return err
	}
	switch frameType {
	case frameRecord:
		record, rErr := wal.ReadRecord(reader)
		if rErr != nil {
			return fmt.Errorf("read record frame: %w", rErr)
		}
		return s.applyRecord(ctx, record)
	case frameHeartbeat:
		lsn, rErr := readUint64(reader)
		if rErr != nil {
			return fmt.Errorf("read heartbeat frame: %w", rErr)
		}
		s.observeMasterLSN(lsn)
		return nil
	case frameSnapshot:
		return s.applySnapshot(ctx, reader)
	default:
		return fmt.Errorf("unknown replication frame %q", frameType)
	}
}

func (s *Standby) applyRecord(ctx context.Context, record wal.Record) error {
	if err := s.applier.ApplyReplicated(ctx, record); err != nil {
		return fmt.Errorf("apply replicated record %d: %w", record.LSN, err)
	}
	s.appliedLSN.Store(record.LSN)
	s.observeMasterLSN(record.LSN)
	return nil
}

// applySnapshot handles a resync: the standby's log was truncated past its
// position, so the master shipped a full snapshot. The standby persists it,
// resets its WAL to the snapshot LSN, and replaces its engine state.
func (s *Standby) applySnapshot(ctx context.Context, reader *bufio.Reader) error {
	lsn, err := readUint64(reader)
	if err != nil {
		return fmt.Errorf("read snapshot lsn: %w", err)
	}
	length, err := readUint64(reader)
	if err != nil {
		return fmt.Errorf("read snapshot length: %w", err)
	}
	if length > maxSnapshotBytes {
		return fmt.Errorf("snapshot length %d exceeds maximum %d", length, maxSnapshotBytes)
	}

	entries, err := parseSnapshot(reader, int64(length)) // #nosec G115 -- length bounded by maxSnapshotBytes above
	if err != nil {
		return fmt.Errorf("parse snapshot: %w", err)
	}

	if err = s.applier.ResetToSnapshot(ctx, s.dir, lsn, entries); err != nil {
		return fmt.Errorf("apply resync snapshot: %w", err)
	}
	s.appliedLSN.Store(lsn)
	s.observeMasterLSN(lsn)
	s.logger.Info("Applied resync snapshot", "lsn", lsn, "entries", len(entries))
	return nil
}

func (s *Standby) observeMasterLSN(lsn uint64) {
	for {
		current := s.masterLSN.Load()
		if lsn <= current {
			return
		}
		if s.masterLSN.CompareAndSwap(current, lsn) {
			return
		}
	}
}

func parseSnapshot(reader *bufio.Reader, length int64) ([]engine.Entry, error) {
	limited := bufio.NewReader(io.LimitReader(reader, length))
	var entries []engine.Entry
	for {
		command, args, err := protocol.ReadCommand(limited, maxSnapshotRecordSize)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, err
		}
		if command != wal.CommandSet || len(args) != 3 {
			return nil, fmt.Errorf("invalid snapshot record %q with %d args", command, len(args))
		}
		entries = append(entries, engine.Entry{Table: args[0], Key: args[1], Value: args[2]})
	}
	return entries, nil
}

func readUint64(reader *bufio.Reader) (uint64, error) {
	buf := make([]byte, 8)
	if _, err := io.ReadFull(reader, buf); err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint64(buf), nil
}
