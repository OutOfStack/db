package wal

import (
	"bufio"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"slices"

	"github.com/OutOfStack/db/internal/protocol"
)

const snapshotMarker = "DBSNPEND"
const snapshotTrailerSize = checksumSize + len(snapshotMarker)

// ErrInvalidSnapshot identifies an incomplete or corrupt snapshot that cannot replace recovery data.
var ErrInvalidSnapshot = errors.New("invalid snapshot")

// SnapshotSource exposes a stable iteration of the in-memory state.
type SnapshotSource interface {
	Range(fn func(table, key, value string) bool)
}

// WriteSnapshot atomically writes the full state as protocol-encoded SET commands.
func WriteSnapshot(ctx context.Context, dir string, lsn uint64, source SnapshotSource) error {
	return writeSnapshot(ctx, dir, lsn, source, (*os.File).Sync)
}

func writeSnapshot(ctx context.Context, dir string, lsn uint64, source SnapshotSource, syncFile func(*os.File) error) error {
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return fmt.Errorf("create snapshot directory: %w", err)
	}
	name := snapshotFilename(lsn)
	temporary, err := os.CreateTemp(dir, name+"-*.tmp")
	if err != nil {
		return fmt.Errorf("create snapshot: %w", err)
	}
	temporaryName := temporary.Name()
	defer func() { _ = os.Remove(temporaryName) }()

	checksum := crc32.NewIEEE()
	output := io.MultiWriter(temporary, checksum)
	if _, err = io.WriteString(output, snapshotHeader); err == nil {
		err = binary.Write(output, binary.BigEndian, lsn)
	}
	if err == nil {
		err = writeSnapshotRecords(ctx, output, source)
	}
	if err == nil {
		trailer := binary.BigEndian.AppendUint32(nil, checksum.Sum32())
		trailer = append(trailer, snapshotMarker...)
		_, err = temporary.Write(trailer)
	}
	if err != nil {
		_ = temporary.Close()
		return fmt.Errorf("write snapshot: %w", err)
	}
	if err = syncFile(temporary); err != nil {
		_ = temporary.Close()
		return fmt.Errorf("sync snapshot: %w", err)
	}
	if _, err = verifySnapshotFile(temporary, &lsn); err != nil {
		_ = temporary.Close()
		return fmt.Errorf("verify snapshot before publication: %w", err)
	}
	if err = temporary.Close(); err != nil {
		return fmt.Errorf("close snapshot: %w", err)
	}
	target := filepath.Join(dir, name)
	if err = os.Rename(temporaryName, target); err != nil {
		return fmt.Errorf("publish snapshot: %w", err)
	}
	if err = SyncDirectory(dir); err != nil {
		return fmt.Errorf("sync snapshot directory: %w", err)
	}

	if err = VerifySnapshot(dir, lsn); err != nil {
		return err
	}
	return removeOldSnapshots(dir, lsn)
}

func writeSnapshotRecords(ctx context.Context, file io.Writer, source SnapshotSource) error {
	var writeErr = ctx.Err()
	if writeErr != nil {
		return writeErr
	}
	source.Range(func(table, key, value string) bool {
		if ctx.Err() != nil {
			writeErr = ctx.Err()
			return false
		}
		if err := protocol.WriteCommand(file, CommandSet, []string{table, key, value}); err != nil {
			writeErr = err
			return false
		}
		return true
	})
	if writeErr != nil {
		return fmt.Errorf("write snapshot: %w", writeErr)
	}
	return nil
}

func removeOldSnapshots(dir string, lsn uint64) error {
	snapshots, err := listNumberedFiles(dir, SnapshotPrefix, SnapshotSuffix)
	if err != nil {
		return fmt.Errorf("list old snapshots: %w", err)
	}
	for _, snapshot := range snapshots {
		if snapshot.number < lsn {
			if err = os.Remove(snapshot.path); err != nil && !os.IsNotExist(err) {
				return fmt.Errorf("remove old snapshot: %w", err)
			}
		}
	}
	if err = SyncDirectory(dir); err != nil {
		return fmt.Errorf("sync snapshot cleanup: %w", err)
	}
	return nil
}

// LoadLatestSnapshot loads the newest verified snapshot. Rejected snapshots are skipped only when the retained WAL
// covers their state, so corruption after pruning cannot silently start an empty or incomplete database.
func LoadLatestSnapshot(dir string, apply func(table, key, value string) error) (uint64, error) {
	snapshots, err := listNumberedFiles(dir, SnapshotPrefix, SnapshotSuffix)
	if err != nil {
		return 0, fmt.Errorf("list snapshots: %w", err)
	}
	var rejectedLSN uint64
	for _, snapshot := range slices.Backward(snapshots) {
		file, openErr := os.Open(snapshot.path)
		if openErr != nil {
			return 0, fmt.Errorf("open snapshot: %w", openErr)
		}
		bodySize, verifyErr := verifySnapshotFile(file, &snapshot.number)
		if errors.Is(verifyErr, ErrInvalidSnapshot) {
			_ = file.Close()
			if snapshot.number > rejectedLSN {
				rejectedLSN = snapshot.number
			}
			continue
		}
		if verifyErr == nil {
			verifyErr = requireSnapshotCoverage(dir, snapshot.number, rejectedLSN)
		}
		if verifyErr == nil {
			verifyErr = applySnapshotFile(file, bodySize, apply)
		}
		closeErr := file.Close()
		if verifyErr != nil {
			return 0, fmt.Errorf("read snapshot %s: %w", snapshot.path, verifyErr)
		}
		if closeErr != nil {
			return 0, fmt.Errorf("close snapshot: %w", closeErr)
		}
		return snapshot.number, nil
	}
	return 0, requireSnapshotCoverage(dir, 0, rejectedLSN)
}

// requireSnapshotCoverage checks without repairing files or applying state. The actual replay can then recover from
// the older snapshot (or LSN zero) without hiding data that existed only in a rejected snapshot.
func requireSnapshotCoverage(dir string, afterLSN, requiredLSN uint64) error {
	if requiredLSN <= afterLSN {
		return nil
	}
	last := afterLSN
	err := ReadRecordsFrom(dir, afterLSN+1, func(record Record) error {
		if record.LSN != last+1 {
			return fmt.Errorf("non-contiguous WAL LSN: got %d after %d", record.LSN, last)
		}
		last = record.LSN
		return nil
	})
	if err != nil {
		return err
	}
	if last < requiredLSN {
		return fmt.Errorf("%w: retained WAL ends at LSN %d, cannot replace rejected snapshot at LSN %d",
			ErrInvalidSnapshot, last, requiredLSN)
	}
	return nil
}

// VerifySnapshot validates the format, LSN, checksum, completion marker, and records before WAL pruning.
func VerifySnapshot(dir string, lsn uint64) error {
	// #nosec G304 -- path uses the configured directory and a generated numeric filename.
	file, err := os.Open(filepath.Join(dir, snapshotFilename(lsn)))
	if err != nil {
		return fmt.Errorf("open snapshot for verification: %w", err)
	}
	defer func() { _ = file.Close() }()
	_, err = verifySnapshotFile(file, &lsn)
	return err
}

func syncSnapshot(dir string, lsn uint64) error {
	// #nosec G304 -- path uses the configured directory and a generated numeric filename.
	file, err := os.Open(filepath.Join(dir, snapshotFilename(lsn)))
	if err != nil {
		return fmt.Errorf("open snapshot for sync: %w", err)
	}
	syncErr := file.Sync()
	closeErr := file.Close()
	if err = errors.Join(syncErr, closeErr); err != nil {
		return fmt.Errorf("sync snapshot before prune: %w", err)
	}
	if err = SyncDirectory(dir); err != nil {
		return fmt.Errorf("sync snapshot directory before prune: %w", err)
	}
	return nil
}

func verifySnapshotFile(file *os.File, expectedLSN *uint64) (int64, error) {
	header := make([]byte, len(snapshotHeader))
	n, err := file.ReadAt(header, 0)
	if err != nil && !errors.Is(err, io.EOF) {
		return 0, err
	}
	if n < len(header) {
		return 0, fmt.Errorf("%w: incomplete header", ErrInvalidSnapshot)
	}
	if string(header) != snapshotHeader {
		return 0, ErrUnsupportedFormat
	}
	info, err := file.Stat()
	if err != nil {
		return 0, err
	}
	bodySize := info.Size() - int64(snapshotTrailerSize)
	if bodySize < int64(len(snapshotHeader)+lsnSize) {
		return 0, fmt.Errorf("%w: missing trailer", ErrInvalidSnapshot)
	}
	trailer := make([]byte, snapshotTrailerSize)
	if _, err = file.ReadAt(trailer, bodySize); err != nil {
		return 0, err
	}
	if string(trailer[checksumSize:]) != snapshotMarker {
		return 0, fmt.Errorf("%w: missing completion marker", ErrInvalidSnapshot)
	}
	checksum := crc32.NewIEEE()
	if _, err = io.Copy(checksum, io.NewSectionReader(file, 0, bodySize)); err != nil {
		return 0, err
	}
	if checksum.Sum32() != binary.BigEndian.Uint32(trailer[:checksumSize]) {
		return 0, fmt.Errorf("%w: checksum mismatch", ErrInvalidSnapshot)
	}
	lsnBytes := make([]byte, lsnSize)
	if _, err = file.ReadAt(lsnBytes, int64(len(snapshotHeader))); err != nil {
		return 0, err
	}
	if expectedLSN != nil && binary.BigEndian.Uint64(lsnBytes) != *expectedLSN {
		return 0, fmt.Errorf("%w: snapshot LSN does not match filename", ErrInvalidSnapshot)
	}
	if err = applySnapshotFile(file, bodySize, func(_, _, _ string) error { return nil }); err != nil {
		return 0, fmt.Errorf("%w: %w", ErrInvalidSnapshot, err)
	}
	return bodySize, nil
}

// ReadSnapshot verifies the entire snapshot before invoking apply. Non-seekable replication input is spooled to a
// temporary file so verification does not require a second in-memory copy of the database.
func ReadSnapshot(reader *bufio.Reader, apply func(table, key, value string) error) error {
	file, err := os.CreateTemp("", "db-snapshot-*.tmp")
	if err != nil {
		return fmt.Errorf("create snapshot verification file: %w", err)
	}
	defer func() { _ = file.Close(); _ = os.Remove(file.Name()) }()
	if _, err = io.Copy(file, reader); err != nil {
		return fmt.Errorf("read snapshot: %w", err)
	}
	bodySize, err := verifySnapshotFile(file, nil)
	if err != nil {
		return err
	}
	return applySnapshotFile(file, bodySize, apply)
}

func applySnapshotFile(file *os.File, bodySize int64, apply func(table, key, value string) error) error {
	offset := int64(len(snapshotHeader) + lsnSize)
	reader := bufio.NewReader(io.NewSectionReader(file, offset, bodySize-offset))
	for {
		if _, err := reader.Peek(1); errors.Is(err, io.EOF) {
			return nil
		} else if err != nil {
			return err
		}
		command, args, readErr := protocol.ReadCommand(reader, maxRecordSize)
		if readErr != nil {
			return readErr
		}
		if command != CommandSet || len(args) != 3 {
			return fmt.Errorf("invalid snapshot record %q with %d arguments", command, len(args))
		}
		if err := apply(args[0], args[1], args[2]); err != nil {
			return fmt.Errorf("apply snapshot: %w", err)
		}
	}
}
