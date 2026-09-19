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
	"sync"
	"sync/atomic"
	"time"

	"github.com/OutOfStack/db/internal/engine"
	"github.com/OutOfStack/db/internal/wal"
)

// Applier persists and applies replicated records. It is satisfied by *storage.Storage, which routes replicated writes
// through the same lock as snapshots so a snapshot never captures a state ahead of the engine.
type Applier interface {
	// ApplyReplicated persists a record and applies it to the engine.
	ApplyReplicated(ctx context.Context, record wal.Record) error
	// ResetToSnapshot replaces all state with a resync snapshot at lsn.
	ResetToSnapshot(ctx context.Context, dir string, lsn uint64, entries []engine.Entry) error
	// Fence puts the storage into a terminal state in which every command, reads included, is refused with err until
	// the process restarts. A standby calls it when its state can no longer be trusted to match the master's.
	Fence(err error)
}

// ErrTerminal marks a standby that has stopped replicating for good: its resync failed part-way, or its history
// diverged from the master's. The standby fences its storage so it serves nothing, and the operator restarts it — after
// a divergence, with an emptied data directory so it reseeds from the master.
var ErrTerminal = errors.New("standby is in a terminal state")

// defaultReconnectBackoff is the pause between replication reconnect attempts.
const defaultReconnectBackoff = time.Second

// Standby connects to a master, persists the streamed WAL to its own log, and applies it to its engine in order. It
// reconnects with backoff and tracks the applied LSN and lag so a promoted standby has a complete, contiguous log.
type Standby struct {
	masterAddr string
	applier    Applier
	dir        string
	logger     *slog.Logger
	backoff    time.Duration
	dialer     *net.Dialer
	limits     settings

	appliedLSN atomic.Uint64
	masterLSN  atomic.Uint64
	connected  atomic.Bool

	terminalMu  sync.Mutex
	terminalErr error

	cancel context.CancelFunc
	done   chan struct{}
}

// NewStandby creates a standby that replicates from masterAddr into applier (the server's storage). dir is the
// WAL/snapshot directory. appliedLSN is the last LSN already durable, used as the resume point in the first handshake.
func NewStandby(
	masterAddr string,
	applier Applier,
	dir string,
	appliedLSN uint64,
	backoff time.Duration,
	logger *slog.Logger,
	options ...Option,
) *Standby {
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	if backoff <= 0 {
		backoff = defaultReconnectBackoff
	}
	limits := defaultSettings()
	for _, option := range options {
		option(&limits)
	}
	s := &Standby{
		masterAddr: masterAddr,
		applier:    applier,
		dir:        dir,
		logger:     logger,
		backoff:    backoff,
		dialer:     &net.Dialer{Timeout: limits.handshakeTimeout},
		limits:     limits,
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

// Lag returns how many LSNs the standby trails the master by, based on the latest record or heartbeat seen. It is a
// best-effort, eventually-consistent estimate given asynchronous replication.
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

// Terminal returns the error that stopped replication for good, or nil while the standby is still replicating or
// retrying. The error wraps ErrTerminal and says what the operator has to do.
func (s *Standby) Terminal() error {
	s.terminalMu.Lock()
	defer s.terminalMu.Unlock()
	return s.terminalErr
}

// fail enters the terminal state (the first cause wins), fences the storage, and returns the terminal error.
func (s *Standby) fail(cause error) error {
	s.terminalMu.Lock()
	defer s.terminalMu.Unlock()
	if s.terminalErr == nil {
		s.terminalErr = fmt.Errorf("%w: %w", ErrTerminal, cause)
		s.applier.Fence(s.terminalErr)
	}
	return s.terminalErr
}

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
		if terminal := s.Terminal(); terminal != nil {
			s.logger.Error("Replication stopped; the standby serves nothing until it is restarted", "error", terminal)
			return
		}
		if err != nil {
			s.logger.Warn("Replication disconnected, retrying", "backoff", s.backoff)
			s.logger.Debug("Replication disconnect details", "error", err)
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

	if err = conn.SetWriteDeadline(time.Now().Add(s.limits.handshakeTimeout)); err != nil {
		return err
	}
	if err = writeHandshake(conn, s.appliedLSN.Load()); err != nil {
		return fmt.Errorf("send handshake: %w", err)
	}
	s.connected.Store(true)
	defer s.connected.Store(false)
	s.logger.Info("Replicating from master", "master", s.masterAddr, "from_lsn", s.appliedLSN.Load())

	reader := bufio.NewReader(deadlineConn{Conn: conn, timeout: s.limits.idleTimeout})
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
		return s.observeMasterLSN(lsn)
	case frameSnapshot:
		return s.applySnapshot(ctx, reader)
	case frameError:
		message, rErr := readErrorFrame(reader)
		if rErr != nil {
			return fmt.Errorf("read error frame: %w", rErr)
		}
		return s.fail(fmt.Errorf("master refused replication: %s", message))
	default:
		return fmt.Errorf("unknown replication frame %q", frameType)
	}
}

// applyRecord persists and applies one streamed record. The master only ever sends the record after the one the
// standby asked for, so a record at or below the applied LSN means the two logs no longer agree on what that LSN
// holds; the standby fails closed rather than skip or re-apply it.
func (s *Standby) applyRecord(ctx context.Context, record wal.Record) error {
	applied := s.appliedLSN.Load()
	if record.LSN <= applied {
		return s.fail(divergence(fmt.Sprintf("master sent LSN %d but this standby has already applied %d", record.LSN, applied)))
	}
	if err := s.applier.ApplyReplicated(ctx, record); err != nil {
		return fmt.Errorf("apply replicated record %d: %w", record.LSN, err)
	}
	s.appliedLSN.Store(record.LSN)
	return s.observeMasterLSN(record.LSN)
}

// applySnapshot handles a resync: the standby's log was truncated past its position, so the master shipped a full
// snapshot. The frame is bounded and verified in full before any state changes; once the standby starts replacing its
// WAL, snapshot, and engine state, a failure leaves it terminal, since the three may no longer agree.
func (s *Standby) applySnapshot(ctx context.Context, reader *bufio.Reader) error {
	lsn, err := readUint64(reader)
	if err != nil {
		return fmt.Errorf("read snapshot lsn: %w", err)
	}
	length, err := readUint64(reader)
	if err != nil {
		return fmt.Errorf("read snapshot length: %w", err)
	}
	if length > uint64(s.limits.maxSnapshotBytes) { // #nosec G115 -- maxSnapshotBytes is positive
		err = fmt.Errorf("snapshot of %d bytes exceeds replication.max_snapshot_size (%d bytes)", length, s.limits.maxSnapshotBytes)
		s.logger.Error("Resync refused: snapshot exceeds the configured bound", "error", err)
		return err
	}

	entries, err := s.parseSnapshot(reader, lsn, int64(length)) // #nosec G115 -- length bounded above
	if err != nil {
		s.logger.Error("Resync snapshot rejected before any state changed", "lsn", lsn, "error", err)
		return fmt.Errorf("parse snapshot: %w", err)
	}

	if err = s.applier.ResetToSnapshot(ctx, s.dir, lsn, entries); err != nil {
		return s.fail(fmt.Errorf("resync at LSN %d failed after the standby began replacing its state: %w; "+
			"restart the standby to reseed from the master", lsn, err))
	}
	s.appliedLSN.Store(lsn)
	s.logger.Info("Applied resync snapshot", "lsn", lsn, "entries", len(entries))
	return s.observeMasterLSN(lsn)
}

// observeMasterLSN records the master's latest LSN for lag reporting. A master behind this standby has a shorter
// history than the one the standby applied, which no amount of streaming can reconcile.
func (s *Standby) observeMasterLSN(lsn uint64) error {
	if applied := s.appliedLSN.Load(); lsn < applied {
		return s.fail(divergence(fmt.Sprintf("master LSN %d is behind this standby's applied LSN %d", lsn, applied)))
	}
	for {
		current := s.masterLSN.Load()
		if lsn <= current {
			return nil
		}
		if s.masterLSN.CompareAndSwap(current, lsn) {
			return nil
		}
	}
}

func divergence(detail string) error {
	return fmt.Errorf("%s: histories diverged; remove this standby's data directory and restart it to reseed from the master",
		detail)
}

func (s *Standby) parseSnapshot(reader *bufio.Reader, lsn uint64, length int64) ([]engine.Entry, error) {
	var entries []engine.Entry
	err := wal.ReadSnapshot(reader, lsn, length, func(table, key, value string) error {
		if len(entries) >= s.limits.maxSnapshotEntries {
			return fmt.Errorf("snapshot exceeds replication.max_snapshot_entries (%d)", s.limits.maxSnapshotEntries)
		}
		entries = append(entries, engine.Entry{Table: table, Key: key, Value: value})
		return nil
	})
	return entries, err
}

func readUint64(reader *bufio.Reader) (uint64, error) {
	buf := make([]byte, 8)
	if _, err := io.ReadFull(reader, buf); err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint64(buf), nil
}
