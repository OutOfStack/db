package wal

import (
	"context"
	"os"
)

// WriteSnapshotWithSync allows external tests to inject snapshot fsync failures without changing production hooks.
func WriteSnapshotWithSync(ctx context.Context, dir string, lsn uint64, source SnapshotSource, syncFile func(*os.File) error) error {
	return writeSnapshot(ctx, dir, lsn, source, syncFile)
}
