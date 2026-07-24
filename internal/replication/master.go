package replication

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"sync"
	"time"

	"github.com/OutOfStack/db/internal/wal"
)

// defaultHeartbeatInterval is how often the master emits a heartbeat frame to an
// otherwise-idle standby so it can keep its lag estimate current.
const defaultHeartbeatInterval = time.Second

// Master streams the WAL to connecting standbys. It combines historical segment
// files on disk with a live fan-out from the WAL writer, so a standby resumes
// from any LSN: a fresh or lagging standby catches up from segments (or a
// snapshot when its position was already truncated), then tails live commits.
type Master struct {
	writer   *wal.Writer
	dir      string
	listener net.Listener
	logger   *slog.Logger

	heartbeatInterval time.Duration
	wg                sync.WaitGroup
}

// NewMaster starts listening for standby connections on listenAddr. writer and
// dir are the server's live WAL writer and its data directory.
func NewMaster(listenAddr string, writer *wal.Writer, dir string, logger *slog.Logger) (*Master, error) {
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	lc := net.ListenConfig{}
	listener, err := lc.Listen(context.Background(), "tcp", listenAddr)
	if err != nil {
		return nil, fmt.Errorf("start replication listener: %w", err)
	}
	return &Master{
		writer:            writer,
		dir:               dir,
		listener:          listener,
		logger:            logger,
		heartbeatInterval: defaultHeartbeatInterval,
	}, nil
}

// Addr returns the address the master is listening on for standbys.
func (m *Master) Addr() net.Addr { return m.listener.Addr() }

// Serve accepts standby connections until ctx is cancelled or Close is called.
func (m *Master) Serve(ctx context.Context) {
	for {
		conn, err := m.listener.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				return
			}
			m.logger.Error("Replication accept failed", "error", err)
			continue
		}
		m.wg.Go(func() {
			m.handleConn(ctx, conn)
		})
	}
}

// Close stops accepting connections and waits for in-flight streams to end.
func (m *Master) Close() error {
	err := m.listener.Close()
	m.wg.Wait()
	return err
}

func (m *Master) handleConn(ctx context.Context, conn net.Conn) {
	defer func() { _ = conn.Close() }()

	// Close the connection when the server shuts down so a blocked stream unblocks.
	stop := context.AfterFunc(ctx, func() { _ = conn.Close() })
	defer stop()

	reader := bufio.NewReader(conn)
	requestedLSN, err := readHandshake(reader)
	if err != nil {
		m.logger.Warn("Replication handshake failed", "remote", conn.RemoteAddr(), "error", err)
		return
	}
	m.logger.Info("Standby connected", "remote", conn.RemoteAddr(), "from_lsn", requestedLSN)

	writer := bufio.NewWriter(conn)
	if err = m.stream(ctx, writer, requestedLSN); err != nil && !errors.Is(err, context.Canceled) {
		m.logger.Info("Replication stream ended", "remote", conn.RemoteAddr(), "error", err)
	}
}

func (m *Master) stream(ctx context.Context, w *bufio.Writer, requestedLSN uint64) error {
	sub, unsub := m.writer.Subscribe()
	defer unsub()

	nextLSN := requestedLSN + 1
	oldest, err := wal.OldestRecordLSN(m.dir, m.writer.LastLSN()+1)
	if err != nil {
		return err
	}
	if nextLSN < oldest {
		snapLSN, serr := m.sendSnapshot(w)
		if serr != nil {
			return serr
		}
		if snapLSN+1 > nextLSN {
			nextLSN = snapLSN + 1
		}
	}

	ticker := time.NewTicker(m.heartbeatInterval)
	defer ticker.Stop()

	for {
		if err = m.streamDisk(w, &nextLSN); err != nil {
			return err
		}
		if err = w.Flush(); err != nil {
			return err
		}
		gap, cErr := m.consumeLive(ctx, w, sub, ticker, &nextLSN)
		if cErr != nil {
			return cErr
		}
		if !gap {
			return nil
		}
	}
}

// consumeLive streams live records until a gap is detected (a record was dropped
// from the buffered fan-out), the context ends, or a write fails. A returned
// gap==true tells the caller to re-scan disk segments to recover the missed
// records before resuming the live tail.
func (m *Master) consumeLive(
	ctx context.Context,
	w *bufio.Writer,
	sub <-chan wal.Record,
	ticker *time.Ticker,
	nextLSN *uint64,
) (bool, error) {
	for {
		select {
		case <-ctx.Done():
			return false, nil
		case <-ticker.C:
			if err := writeHeartbeatFrame(w, m.writer.LastLSN()); err != nil {
				return false, err
			}
			if err := w.Flush(); err != nil {
				return false, err
			}
			// A record can be dropped from the buffered fan-out during a burst; if
			// the burst then stops, no later record arrives to reveal the gap. The
			// heartbeat is our backstop: whenever the committed tail is ahead of
			// what we have shipped, recover the missing records from disk.
			if m.writer.LastLSN() >= *nextLSN {
				return true, nil
			}
		case record, ok := <-sub:
			if !ok {
				return false, nil
			}
			if record.LSN < *nextLSN {
				continue // already sent from disk
			}
			if record.LSN > *nextLSN {
				return true, nil // gap: recover from disk
			}
			if err := writeRecordFrame(w, record); err != nil {
				return false, err
			}
			if err := w.Flush(); err != nil {
				return false, err
			}
			*nextLSN++
		}
	}
}

func (m *Master) streamDisk(w *bufio.Writer, nextLSN *uint64) error {
	return wal.ReadRecordsFrom(m.dir, *nextLSN, func(record wal.Record) error {
		if record.LSN < *nextLSN {
			return nil
		}
		if err := writeRecordFrame(w, record); err != nil {
			return err
		}
		*nextLSN = record.LSN + 1
		return nil
	})
}

func (m *Master) sendSnapshot(w *bufio.Writer) (uint64, error) {
	lsn, path, ok, err := wal.LatestSnapshotInfo(m.dir)
	if err != nil {
		return 0, err
	}
	if !ok {
		return 0, errors.New("standby needs resync but master has no snapshot")
	}
	file, err := os.Open(path) // #nosec G304 -- path comes from the WAL directory listing
	if err != nil {
		return 0, fmt.Errorf("open snapshot: %w", err)
	}
	defer func() { _ = file.Close() }()
	info, err := file.Stat()
	if err != nil {
		return 0, fmt.Errorf("stat snapshot: %w", err)
	}
	if err = writeSnapshotFrame(w, lsn, info.Size(), file); err != nil {
		return 0, err
	}
	if err = w.Flush(); err != nil {
		return 0, err
	}
	m.logger.Info("Sent snapshot to standby", "lsn", lsn, "bytes", info.Size())
	return lsn, nil
}
