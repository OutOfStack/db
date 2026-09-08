package wal

import (
	"errors"
	"fmt"
	"io"
)

// OldestRecordLSN returns the LSN of the oldest record still retained on disk. It is the first LSN of the earliest WAL
// segment; when no segments exist it returns fallback (the caller passes LastLSN+1, meaning "nothing on disk").
func OldestRecordLSN(dir string, fallback uint64) (uint64, error) {
	segments, err := listNumberedFiles(dir, WALPrefix, WALSuffix)
	if err != nil {
		return 0, fmt.Errorf("list WAL segments: %w", err)
	}
	if len(segments) == 0 {
		return fallback, nil
	}
	return segments[0].number, nil
}

// LatestSnapshotInfo returns the LSN and path of the newest snapshot on disk. ok is false when no snapshot exists.
func LatestSnapshotInfo(dir string) (lsn uint64, path string, ok bool, err error) {
	snapshots, err := listNumberedFiles(dir, SnapshotPrefix, SnapshotSuffix)
	if err != nil {
		return 0, "", false, fmt.Errorf("list snapshots: %w", err)
	}
	if len(snapshots) == 0 {
		return 0, "", false, nil
	}
	latest := snapshots[len(snapshots)-1]
	return latest.number, latest.path, true, nil
}

// ReadRecordsFrom streams records with LSN >= fromLSN from the on-disk segments to fn, in LSN order. It is used by the
// replication master to catch a standby up from segment files. An incomplete EOF read in the final segment ends
// iteration cleanly because the master may still be writing it. Checksum mismatches always return an error.
func ReadRecordsFrom(dir string, fromLSN uint64, fn func(Record) error) error {
	segments, err := listNumberedFiles(dir, WALPrefix, WALSuffix)
	if err != nil {
		return fmt.Errorf("list WAL segments: %w", err)
	}
	for index, segment := range segments {
		isLast := index == len(segments)-1
		if err = readSegmentRecords(segment, isLast, fromLSN, fn); err != nil {
			return err
		}
	}
	return nil
}

func readSegmentRecords(segment numberedFile, isLast bool, fromLSN uint64, fn func(Record) error) error {
	file, reader, err := openWALSegment(segment.path)
	if err != nil {
		return fmt.Errorf("open WAL segment %s: %w", segment.path, err)
	}
	defer func() { _ = file.Close() }()

	for {
		offset, seekErr := file.Seek(0, io.SeekCurrent)
		if seekErr != nil {
			return fmt.Errorf("find WAL offset: %w", seekErr)
		}
		offset -= int64(reader.Buffered())
		record, readErr := readRecord(reader)
		if errors.Is(readErr, io.EOF) && !errors.Is(readErr, ErrPartialRecord) {
			return nil
		}
		if readErr != nil {
			if isLast && errors.Is(readErr, ErrPartialRecord) {
				return nil
			}
			return fmt.Errorf("read WAL segment %s at offset %d: %w", segment.path, offset, readErr)
		}
		if record.LSN < fromLSN {
			continue
		}
		if err = fn(record); err != nil {
			return err
		}
	}
}
