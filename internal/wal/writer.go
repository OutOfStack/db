package wal

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/OutOfStack/db/internal/protocol"
)

// SyncPolicy controls when appended WAL records are fsynced.
type SyncPolicy string

const (
	SyncAlways   SyncPolicy = "always"
	SyncEverySec SyncPolicy = "everysec"
	SyncNo       SyncPolicy = "no"
)

// ErrClosed is returned when an operation is attempted after shutdown starts.
var ErrClosed = errors.New("wal writer is closed")

// WriterConfig configures the segmented WAL writer.
type WriterConfig struct {
	Dir         string
	Sync        SyncPolicy
	SegmentSize int64
}

type requestKind uint8

const (
	requestAppend requestKind = iota
	requestAppendReplicated
	requestPrune
	requestReset
	requestClose
)

type writerRequest struct {
	kind    requestKind
	command string
	args    []string
	record  Record // used by requestAppendReplicated, which carries an explicit LSN
	uptoLSN uint64
	result  chan writerResult
}

// subscriberBuffer bounds how many committed records the writer can queue for a
// single replication subscriber before dropping. A dropped record is not lost:
// the master streaming loop detects the LSN gap and re-reads it from the WAL
// segments on disk, so a slow standby never blocks the writer goroutine.
const subscriberBuffer = 1024

type subscriber struct {
	ch chan Record
}

type writerResult struct {
	lsn uint64
	err error
}

// Writer serializes WAL appends through one goroutine.
type Writer struct {
	config WriterConfig
	file   *os.File
	size   int64

	requests  chan writerRequest
	done      chan struct{}
	closing   atomic.Bool
	lastLSN   atomic.Uint64
	closeOnce sync.Once
	closeErr  error

	subsMu sync.RWMutex
	subs   map[*subscriber]struct{}
}

// OpenWriter opens a writer after recovery. lastLSN must be the last LSN
// returned by snapshot loading plus WAL replay.
func OpenWriter(config WriterConfig, lastLSN uint64) (*Writer, error) {
	if config.Dir == "" {
		return nil, errors.New("WAL directory cannot be empty")
	}
	if config.SegmentSize <= 0 {
		return nil, errors.New("WAL segment size must be positive")
	}
	switch config.Sync {
	case SyncAlways, SyncEverySec, SyncNo:
	default:
		return nil, fmt.Errorf("invalid WAL sync policy %q", config.Sync)
	}
	if err := os.MkdirAll(config.Dir, 0o750); err != nil {
		return nil, fmt.Errorf("create WAL directory: %w", err)
	}
	segments, err := listNumberedFiles(config.Dir, walPrefix, walSuffix)
	if err != nil {
		return nil, fmt.Errorf("list WAL segments: %w", err)
	}
	var file *os.File
	var size int64
	if len(segments) > 0 {
		last := segments[len(segments)-1]
		file, err = os.OpenFile(last.path, os.O_WRONLY|os.O_APPEND, 0o600)
		if err != nil {
			return nil, fmt.Errorf("open WAL segment: %w", err)
		}
		info, statErr := file.Stat()
		if statErr != nil {
			_ = file.Close()
			return nil, fmt.Errorf("stat WAL segment: %w", statErr)
		}
		size = info.Size()
	}

	writer := &Writer{
		config:   config,
		file:     file,
		size:     size,
		requests: make(chan writerRequest, 256),
		done:     make(chan struct{}),
		subs:     make(map[*subscriber]struct{}),
	}
	writer.lastLSN.Store(lastLSN)
	go writer.run()
	return writer, nil
}

// Append writes one mutation and waits until it satisfies the configured sync policy.
func (w *Writer) Append(ctx context.Context, command string, args []string) (uint64, error) {
	if w.closing.Load() {
		return 0, ErrClosed
	}
	if err := validateRecord(Record{Command: command, Args: args}); err != nil {
		return 0, err
	}
	// Reject records the recovery reader could not decode (its RESP limit is
	// maxRecordSize). The network layer's max message size is configurable and
	// may exceed this, so guard here rather than persisting an unreadable record.
	if size := protocol.CommandSize(command, args); size > maxRecordSize {
		return 0, fmt.Errorf("WAL record size %d exceeds maximum %d bytes", size, maxRecordSize)
	}
	result := make(chan writerResult, 1)
	request := writerRequest{kind: requestAppend, command: command, args: append([]string(nil), args...), result: result}
	select {
	case w.requests <- request:
	case <-ctx.Done():
		return 0, ctx.Err()
	case <-w.done:
		return 0, ErrClosed
	}
	// Once the request is enqueued the writer will assign it a durable LSN, so we
	// must wait for that outcome rather than abandoning it on ctx cancellation:
	// an orphaned LSN would stall the caller's in-order apply of later records.
	select {
	case response := <-result:
		return response.lsn, response.err
	case <-w.done:
		select {
		case response := <-result:
			return response.lsn, response.err
		default:
			return 0, ErrClosed
		}
	}
}

// AppendRecord writes a record with a caller-assigned LSN. Standbys use it to
// persist records received from the master's replication stream, preserving the
// master's LSNs so a promoted standby continues the same log. The record's LSN
// must be exactly the current LastLSN+1.
func (w *Writer) AppendRecord(ctx context.Context, record Record) error {
	if w.closing.Load() {
		return ErrClosed
	}
	if err := validateRecord(record); err != nil {
		return err
	}
	if size := protocol.CommandSize(record.Command, record.Args); size > maxRecordSize {
		return fmt.Errorf("WAL record size %d exceeds maximum %d bytes", size, maxRecordSize)
	}
	return w.control(ctx, writerRequest{kind: requestAppendReplicated, record: record})
}

// Reset discards all WAL segments and sets LastLSN to lsn, so the next appended
// record must be lsn+1. Standbys call it after loading a snapshot at lsn during
// resync, when their existing log is entirely superseded by the snapshot.
func (w *Writer) Reset(ctx context.Context, lsn uint64) error {
	return w.control(ctx, writerRequest{kind: requestReset, uptoLSN: lsn})
}

// Subscribe registers for committed records. The returned channel receives every
// record the writer commits after the call; the returned function unsubscribes.
// Sends are non-blocking (see subscriberBuffer): a subscriber that cannot keep up
// misses records and must recover them by reading segments from disk.
func (w *Writer) Subscribe() (<-chan Record, func()) {
	sub := &subscriber{ch: make(chan Record, subscriberBuffer)}
	w.subsMu.Lock()
	w.subs[sub] = struct{}{}
	w.subsMu.Unlock()
	return sub.ch, func() {
		w.subsMu.Lock()
		delete(w.subs, sub)
		w.subsMu.Unlock()
	}
}

// publish delivers a committed record to every subscriber without blocking.
func (w *Writer) publish(record Record) {
	w.subsMu.RLock()
	defer w.subsMu.RUnlock()
	for sub := range w.subs {
		select {
		case sub.ch <- record:
		default:
		}
	}
}

// LastLSN returns the most recently written LSN.
func (w *Writer) LastLSN() uint64 { return w.lastLSN.Load() }

// Prune removes segments whose records are all represented by a snapshot.
func (w *Writer) Prune(ctx context.Context, uptoLSN uint64) error {
	return w.control(ctx, writerRequest{kind: requestPrune, uptoLSN: uptoLSN})
}

// Close flushes, fsyncs, and closes the WAL. It is safe to call more than once.
func (w *Writer) Close() error {
	w.closeOnce.Do(func() {
		w.closing.Store(true)
		result := make(chan writerResult, 1)
		select {
		case w.requests <- writerRequest{kind: requestClose, result: result}:
			response := <-result
			w.closeErr = response.err
		case <-w.done:
		}
	})
	<-w.done
	return w.closeErr
}

func (w *Writer) control(ctx context.Context, request writerRequest) error {
	if w.closing.Load() {
		return ErrClosed
	}
	request.result = make(chan writerResult, 1)
	select {
	case w.requests <- request:
	case <-ctx.Done():
		return ctx.Err()
	case <-w.done:
		return ErrClosed
	}
	// Once enqueued, wait for the outcome regardless of ctx cancellation: the
	// writer will still process this request and mutate durable state (assign an
	// LSN, remove segments), so abandoning it here would desync a caller that
	// applies records in LSN order — e.g. a standby whose context is cancelled by
	// Stop() mid-append, leaving its engine one record behind its own WAL.
	select {
	case response := <-request.result:
		return response.err
	case <-w.done:
		select {
		case response := <-request.result:
			return response.err
		default:
			return ErrClosed
		}
	}
}

func (w *Writer) run() {
	defer close(w.done)
	state := writerState{file: w.file, size: w.size}

	var ticker *time.Ticker
	var tick <-chan time.Time
	if w.config.Sync == SyncEverySec {
		ticker = time.NewTicker(time.Second)
		tick = ticker.C
		defer ticker.Stop()
	}

	for {
		select {
		case <-tick:
			w.syncEverySecond(&state)
		case request := <-w.requests:
			if request.kind == requestAppend {
				batch, control := w.collectAppendBatch(request)
				w.handleBatch(batch, &state)
				if control != nil && w.handleControl(*control, &state) {
					return
				}
				continue
			}
			if w.handleControl(request, &state) {
				return
			}
		}
	}
}

type writerState struct {
	file        *os.File
	size        int64
	terminalErr error
}

func (w *Writer) syncEverySecond(state *writerState) {
	if state.terminalErr != nil || state.file == nil {
		return
	}
	if err := state.file.Sync(); err != nil {
		state.terminalErr = fmt.Errorf("sync WAL: %w", err)
	}
}

func (w *Writer) collectAppendBatch(first writerRequest) ([]writerRequest, *writerRequest) {
	batch := []writerRequest{first}
	for {
		select {
		case next := <-w.requests:
			if next.kind != requestAppend {
				return batch, &next
			}
			batch = append(batch, next)
		default:
			return batch, nil
		}
	}
}

func (w *Writer) handleBatch(batch []writerRequest, state *writerState) {
	results := make([]writerResult, len(batch))
	wrote := false
	for index, request := range batch {
		if state.terminalErr != nil {
			results[index].err = state.terminalErr
			continue
		}
		lsn, err := w.appendOne(request, state)
		if err != nil {
			state.terminalErr = fmt.Errorf("append WAL record: %w", err)
			results[index].err = state.terminalErr
			continue
		}
		results[index].lsn = lsn
		wrote = true
	}

	// Sync whatever was written even when a later append in the batch failed:
	// the earlier records are already on disk and will replay on restart, so
	// their callers must be acked, not failed. Only a sync failure leaves those
	// records non-durable, and only then do we fail the callers we would ack.
	if wrote && w.config.Sync == SyncAlways && state.file != nil {
		if err := state.file.Sync(); err != nil {
			state.terminalErr = fmt.Errorf("sync WAL: %w", err)
			for index := range results {
				if results[index].err == nil {
					results[index].err = state.terminalErr
				}
			}
		}
	}
	for index, request := range batch {
		request.result <- results[index]
	}

	// Publish only records the callers were acked for; a failed append or sync
	// leaves the record non-durable, so it must not be streamed to standbys.
	for index, request := range batch {
		if results[index].err == nil {
			w.publish(Record{LSN: results[index].lsn, Command: request.command, Args: request.args})
		}
	}
}

func (w *Writer) appendOne(request writerRequest, state *writerState) (uint64, error) {
	lsn := w.lastLSN.Load() + 1
	record, err := encodeRecord(Record{LSN: lsn, Command: request.command, Args: request.args})
	if err != nil {
		return 0, err
	}
	if err = w.ensureSegment(state, lsn, int64(len(record))); err != nil {
		return 0, err
	}
	written, err := state.file.Write(record)
	state.size += int64(written)
	if err != nil {
		return 0, err
	}
	if written != len(record) {
		return 0, io.ErrShortWrite
	}
	w.lastLSN.Store(lsn)
	return lsn, nil
}

// appendReplicated writes a record with its own LSN, enforcing contiguity with
// the current tail. Under a durable sync policy it fsyncs before returning so an
// acked replicated record survives a standby crash, mirroring Append.
func (w *Writer) appendReplicated(record Record, state *writerState) error {
	expected := w.lastLSN.Load() + 1
	if record.LSN != expected {
		return fmt.Errorf("non-contiguous replicated LSN: got %d, want %d", record.LSN, expected)
	}
	encoded, err := encodeRecord(record)
	if err != nil {
		return err
	}
	if err = w.ensureSegment(state, record.LSN, int64(len(encoded))); err != nil {
		return err
	}
	written, err := state.file.Write(encoded)
	state.size += int64(written)
	if err != nil {
		state.terminalErr = fmt.Errorf("append replicated WAL record: %w", err)
		return state.terminalErr
	}
	if written != len(encoded) {
		state.terminalErr = fmt.Errorf("append replicated WAL record: %w", io.ErrShortWrite)
		return state.terminalErr
	}
	if w.config.Sync != SyncNo && state.file != nil {
		if err = state.file.Sync(); err != nil {
			state.terminalErr = fmt.Errorf("sync WAL: %w", err)
			return state.terminalErr
		}
	}
	w.lastLSN.Store(record.LSN)
	return nil
}

// reset removes every WAL segment and sets LastLSN to lsn, so the next record
// appended must be lsn+1 into a fresh segment.
func (w *Writer) reset(state *writerState, lsn uint64) error {
	if state.file != nil {
		if err := state.file.Close(); err != nil {
			return fmt.Errorf("close WAL before reset: %w", err)
		}
		state.file = nil
		state.size = 0
	}
	segments, err := listNumberedFiles(w.config.Dir, walPrefix, walSuffix)
	if err != nil {
		return fmt.Errorf("list WAL segments for reset: %w", err)
	}
	for _, segment := range segments {
		if err = os.Remove(segment.path); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("remove WAL segment during reset: %w", err)
		}
	}
	if err = syncDirectory(w.config.Dir); err != nil {
		return fmt.Errorf("sync WAL directory during reset: %w", err)
	}
	w.lastLSN.Store(lsn)
	return nil
}

func (w *Writer) ensureSegment(state *writerState, lsn uint64, recordSize int64) error {
	if state.file != nil && (state.size == 0 || state.size+recordSize <= w.config.SegmentSize) {
		return nil
	}
	if err := w.closeForRotation(state); err != nil {
		return err
	}
	// The previous segment is closed and durably synced. Drop the handle so a
	// failed open below cannot leave a stale, closed file that later code syncs
	// (which would wrongly fail callers whose records are already durable).
	state.file = nil
	state.size = 0
	path := filepath.Join(w.config.Dir, walFilename(lsn))
	// #nosec G304 -- path is constrained to the configured directory and a generated numeric filename.
	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	if w.config.Sync != SyncNo {
		if err = syncDirectory(w.config.Dir); err != nil {
			_ = file.Close()
			return fmt.Errorf("sync WAL directory: %w", err)
		}
	}
	state.file = file
	state.size = 0
	return nil
}

func (w *Writer) closeForRotation(state *writerState) error {
	if state.file == nil {
		return nil
	}
	// Sync the segment being closed for any durable policy. Under SyncAlways a
	// batch that overflows into a new segment would otherwise close the old
	// segment with its final records unsynced, even though they were acked.
	if w.config.Sync != SyncNo {
		if err := state.file.Sync(); err != nil {
			return err
		}
	}
	return state.file.Close()
}

func (w *Writer) handleControl(request writerRequest, state *writerState) bool {
	switch request.kind {
	case requestAppendReplicated:
		err := state.terminalErr
		if err == nil {
			err = w.appendReplicated(request.record, state)
		}
		request.result <- writerResult{err: err}
		if err == nil {
			w.publish(request.record)
		}
		return false
	case requestReset:
		err := state.terminalErr
		if err == nil {
			err = w.reset(state, request.uptoLSN)
		}
		request.result <- writerResult{err: err}
		return false
	case requestPrune:
		err := state.terminalErr
		if err == nil {
			err = w.prune(state, request.uptoLSN)
		}
		request.result <- writerResult{err: err}
		return false
	case requestClose:
		request.result <- writerResult{err: w.handleClose(state)}
		return true
	default:
		return false
	}
}

func (w *Writer) handleClose(state *writerState) error {
	err := state.terminalErr
	if state.file != nil {
		if syncErr := state.file.Sync(); err == nil && syncErr != nil {
			err = fmt.Errorf("sync WAL during close: %w", syncErr)
		}
		if closeErr := state.file.Close(); err == nil && closeErr != nil {
			err = fmt.Errorf("close WAL: %w", closeErr)
		}
	}
	if syncErr := w.syncAllSegments(); err == nil && syncErr != nil {
		err = syncErr
	}
	return err
}

func (w *Writer) syncAllSegments() error {
	segments, err := listNumberedFiles(w.config.Dir, walPrefix, walSuffix)
	if err != nil {
		return fmt.Errorf("list WAL segments during close: %w", err)
	}
	for _, segment := range segments {
		file, openErr := os.OpenFile(segment.path, os.O_RDWR, 0o600)
		if openErr != nil {
			return fmt.Errorf("open WAL segment during close: %w", openErr)
		}
		syncErr := file.Sync()
		closeErr := file.Close()
		if syncErr != nil {
			return fmt.Errorf("sync WAL segment during close: %w", syncErr)
		}
		if closeErr != nil {
			return fmt.Errorf("close WAL segment during close: %w", closeErr)
		}
	}
	return nil
}

func (w *Writer) prune(state *writerState, uptoLSN uint64) error {
	if state.file != nil {
		if err := state.file.Sync(); err != nil {
			return fmt.Errorf("sync WAL before prune: %w", err)
		}
	}
	segments, err := listNumberedFiles(w.config.Dir, walPrefix, walSuffix)
	if err != nil {
		return fmt.Errorf("list WAL segments for prune: %w", err)
	}
	if w.lastLSN.Load() <= uptoLSN {
		if state.file != nil {
			if err = state.file.Close(); err != nil {
				return fmt.Errorf("close WAL before prune: %w", err)
			}
			state.file = nil
			state.size = 0
		}
		for _, segment := range segments {
			if err = os.Remove(segment.path); err != nil && !os.IsNotExist(err) {
				return fmt.Errorf("remove WAL segment: %w", err)
			}
		}
		return syncDirectory(w.config.Dir)
	}

	for index := 0; index+1 < len(segments); index++ {
		if segments[index+1].number > uptoLSN+1 {
			break
		}
		if err = os.Remove(segments[index].path); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("remove WAL segment: %w", err)
		}
	}
	return syncDirectory(w.config.Dir)
}
