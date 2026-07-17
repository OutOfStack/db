package wal

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/OutOfStack/db/internal/protocol"
)

// SnapshotSource exposes a stable iteration of the in-memory state.
type SnapshotSource interface {
	Range(fn func(table, key, value string) bool)
}

// WriteSnapshot atomically writes the full state as protocol-encoded SET commands.
func WriteSnapshot(ctx context.Context, dir string, lsn uint64, source SnapshotSource) error {
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

	if err = writeSnapshotRecords(ctx, temporary, source); err != nil {
		_ = temporary.Close()
		return err
	}
	if err = temporary.Sync(); err != nil {
		_ = temporary.Close()
		return fmt.Errorf("sync snapshot: %w", err)
	}
	if err = temporary.Close(); err != nil {
		return fmt.Errorf("close snapshot: %w", err)
	}
	target := filepath.Join(dir, name)
	if err = os.Rename(temporaryName, target); err != nil {
		return fmt.Errorf("publish snapshot: %w", err)
	}
	if err = syncDirectory(dir); err != nil {
		return fmt.Errorf("sync snapshot directory: %w", err)
	}

	return removeOldSnapshots(dir, lsn)
}

func writeSnapshotRecords(ctx context.Context, file io.Writer, source SnapshotSource) error {
	var writeErr error
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
	snapshots, err := listNumberedFiles(dir, snapshotPrefix, snapshotSuffix)
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
	if err = syncDirectory(dir); err != nil {
		return fmt.Errorf("sync snapshot cleanup: %w", err)
	}
	return nil
}

// LoadLatestSnapshot loads the newest complete snapshot and returns its LSN.
func LoadLatestSnapshot(dir string, apply func(table, key, value string) error) (uint64, error) {
	snapshots, err := listNumberedFiles(dir, snapshotPrefix, snapshotSuffix)
	if err != nil {
		return 0, fmt.Errorf("list snapshots: %w", err)
	}
	if len(snapshots) == 0 {
		return 0, nil
	}
	latest := snapshots[len(snapshots)-1]
	file, err := os.Open(latest.path)
	if err != nil {
		return 0, fmt.Errorf("open snapshot: %w", err)
	}
	defer func() { _ = file.Close() }()

	reader := bufio.NewReader(file)
	for {
		command, args, readErr := protocol.ReadCommand(reader, maxRecordSize)
		if errors.Is(readErr, io.EOF) {
			break
		}
		if readErr != nil {
			return 0, fmt.Errorf("read snapshot %s: %w", latest.path, readErr)
		}
		if command != CommandSet || len(args) != 3 {
			return 0, fmt.Errorf("invalid snapshot record %q with %d arguments", command, len(args))
		}
		if err = apply(args[0], args[1], args[2]); err != nil {
			return 0, fmt.Errorf("apply snapshot: %w", err)
		}
	}
	return latest.number, nil
}
