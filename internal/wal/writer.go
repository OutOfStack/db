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
	requestPrune
	requestClose
)

type writerRequest struct {
	kind    requestKind
	command string
	args    []string
	uptoLSN uint64
	result  chan writerResult
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
	select {
	case response := <-request.result:
		return response.err
	case <-ctx.Done():
		return ctx.Err()
	case <-w.done:
		return ErrClosed
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
	}

	if state.terminalErr == nil && w.config.Sync == SyncAlways && state.file != nil {
		if err := state.file.Sync(); err != nil {
			state.terminalErr = fmt.Errorf("sync WAL: %w", err)
		}
	}
	if state.terminalErr != nil {
		for index := range results {
			results[index].err = state.terminalErr
		}
	}
	for index, request := range batch {
		request.result <- results[index]
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

func (w *Writer) ensureSegment(state *writerState, lsn uint64, recordSize int64) error {
	if state.file != nil && (state.size == 0 || state.size+recordSize <= w.config.SegmentSize) {
		return nil
	}
	if err := w.closeForRotation(state); err != nil {
		return err
	}
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
	case requestPrune:
		err := state.terminalErr
		if err == nil {
			err = w.prune(state, request.uptoLSN)
		}
		request.result <- writerResult{err: err}
		return false
	case requestClose:
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
		request.result <- writerResult{err: err}
		return true
	default:
		return false
	}
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
