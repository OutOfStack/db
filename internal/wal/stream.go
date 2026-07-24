package wal

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
)

// OldestRecordLSN returns the LSN of the oldest record still retained on disk.
// It is the first LSN of the earliest WAL segment; when no segments exist it
// returns fallback (the caller passes LastLSN+1, meaning "nothing on disk").
func OldestRecordLSN(dir string, fallback uint64) (uint64, error) {
	segments, err := listNumberedFiles(dir, walPrefix, walSuffix)
	if err != nil {
		return 0, fmt.Errorf("list WAL segments: %w", err)
	}
	if len(segments) == 0 {
		return fallback, nil
	}
	return segments[0].number, nil
}

// LatestSnapshotInfo returns the LSN and path of the newest snapshot on disk.
// ok is false when no snapshot exists.
func LatestSnapshotInfo(dir string) (lsn uint64, path string, ok bool, err error) {
	snapshots, err := listNumberedFiles(dir, snapshotPrefix, snapshotSuffix)
	if err != nil {
		return 0, "", false, fmt.Errorf("list snapshots: %w", err)
	}
	if len(snapshots) == 0 {
		return 0, "", false, nil
	}
	latest := snapshots[len(snapshots)-1]
	return latest.number, latest.path, true, nil
}

// ReadRecordsFrom streams records with LSN >= fromLSN from the on-disk segments
// to fn, in LSN order. It is used by the replication master to catch a standby
// up from segment files. Because the master appends concurrently, a partial or
// checksum-invalid record at the tail of the final segment is treated as a
// half-written live record and ends iteration cleanly (the caller streams the
// rest from the live fan-out); the same damage in an earlier segment is an error.
func ReadRecordsFrom(dir string, fromLSN uint64, fn func(Record) error) error {
	segments, err := listNumberedFiles(dir, walPrefix, walSuffix)
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
	file, err := os.Open(segment.path)
	if err != nil {
		return fmt.Errorf("open WAL segment %s: %w", segment.path, err)
	}
	defer func() { _ = file.Close() }()

	reader := bufio.NewReader(file)
	for {
		record, readErr := readRecord(reader)
		if errors.Is(readErr, io.EOF) {
			return nil
		}
		if readErr != nil {
			if isLast && (errors.Is(readErr, ErrPartialRecord) || errors.Is(readErr, ErrChecksum)) {
				return nil
			}
			return fmt.Errorf("read WAL segment %s: %w", segment.path, readErr)
		}
		if record.LSN < fromLSN {
			continue
		}
		if err = fn(record); err != nil {
			return err
		}
	}
}
