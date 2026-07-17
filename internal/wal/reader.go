package wal

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
)

// Reader replays ordered WAL segments from a directory.
type Reader struct {
	dir    string
	logger *slog.Logger
}

// NewReader creates a WAL reader. A nil logger discards recovery warnings.
func NewReader(dir string, logger *slog.Logger) *Reader {
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	return &Reader{dir: dir, logger: logger}
}

// Replay applies records newer than afterLSN and returns the last valid LSN.
// A partial or checksum-invalid record in the final segment is truncated as a
// crash tail; the same damage in an earlier segment is a startup error.
func (r *Reader) Replay(afterLSN uint64, apply func(Record) error) (uint64, error) {
	segments, err := listNumberedFiles(r.dir, walPrefix, walSuffix)
	if err != nil {
		return afterLSN, fmt.Errorf("list WAL segments: %w", err)
	}

	lastSeen := uint64(0)
	lastApplied := afterLSN
	for index, segment := range segments {
		isLast := index == len(segments)-1
		segmentLast, replayErr := r.replaySegment(segment, isLast, afterLSN, lastSeen, lastApplied, apply)
		if replayErr != nil {
			return lastApplied, replayErr
		}
		lastSeen = segmentLast.seen
		lastApplied = segmentLast.applied
	}
	return lastApplied, nil
}

type replayPosition struct {
	seen    uint64
	applied uint64
}

func (r *Reader) replaySegment( //nolint:gocyclo // recovery deliberately keeps corruption decisions beside file-position handling
	segment numberedFile,
	isLast bool,
	afterLSN uint64,
	lastSeen uint64,
	lastApplied uint64,
	apply func(Record) error,
) (replayPosition, error) {
	file, err := os.Open(segment.path)
	if err != nil {
		return replayPosition{}, fmt.Errorf("open WAL segment %s: %w", segment.path, err)
	}
	reader := bufio.NewReader(file)
	position := replayPosition{seen: lastSeen, applied: lastApplied}

	for {
		offset, seekErr := file.Seek(0, io.SeekCurrent)
		if seekErr != nil {
			_ = file.Close()
			return position, fmt.Errorf("find WAL offset: %w", seekErr)
		}
		offset -= int64(reader.Buffered())

		record, readErr := readRecord(reader)
		if errors.Is(readErr, io.EOF) {
			break
		}
		if readErr != nil {
			_ = file.Close()
			if isLast && (errors.Is(readErr, ErrPartialRecord) || errors.Is(readErr, ErrChecksum)) {
				if truncateErr := os.Truncate(segment.path, offset); truncateErr != nil {
					return position, fmt.Errorf("truncate damaged WAL tail: %w", truncateErr)
				}
				r.logger.Warn("Truncated damaged WAL tail", "segment", segment.path, "offset", offset, "error", readErr)
				return position, nil
			}
			return position, fmt.Errorf("corrupt WAL segment %s at offset %d: %w", segment.path, offset, readErr)
		}

		if position.seen != 0 && record.LSN != position.seen+1 {
			_ = file.Close()
			return position, fmt.Errorf("non-contiguous WAL LSN: got %d after %d", record.LSN, position.seen)
		}
		position.seen = record.LSN
		if record.LSN <= afterLSN {
			continue
		}
		if position.applied == afterLSN && record.LSN != afterLSN+1 {
			_ = file.Close()
			return position, fmt.Errorf("WAL starts at LSN %d after snapshot LSN %d", record.LSN, afterLSN)
		}
		if err = apply(record); err != nil {
			_ = file.Close()
			return position, fmt.Errorf("apply WAL record %d: %w", record.LSN, err)
		}
		position.applied = record.LSN
	}

	if err = file.Close(); err != nil {
		return position, fmt.Errorf("close WAL segment: %w", err)
	}
	return position, nil
}
