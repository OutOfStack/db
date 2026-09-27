package wal

import (
	"errors"
	"os"
	"testing"
	"time"

	"github.com/OutOfStack/db/internal/protocol"
	"github.com/stretchr/testify/require"
)

func TestSyncFailureLatchesReadiness(t *testing.T) {
	t.Parallel()
	file, err := os.CreateTemp(t.TempDir(), "wal-sync")
	require.NoError(t, err)
	writer := &Writer{}
	writer.lastLSN.Store(4)
	writer.syncedLSN.Store(3)
	state := writerState{file: file}
	require.NoError(t, file.Close())
	writer.syncEverySecond(&state)
	status := writer.Status()
	require.False(t, status.Ready)
	require.True(t, status.Degraded)
	require.ErrorIs(t, status.TerminalError, os.ErrClosed)
	require.ErrorIs(t, status.TerminalError, ErrTerminal)
	require.Equal(t, uint64(3), status.SyncedLSN)
	result := make(chan writerResult, 1)
	writer.handleBatch([]writerRequest{{result: result}}, &state)
	// A later append is refused as UNAVAILABLE, the code a client sees for a node it cannot write to, while the I/O
	// error that caused it stays reachable.
	appendErr := (<-result).err
	require.ErrorIs(t, appendErr, os.ErrClosed)
	require.Equal(t, protocol.CodeUnavailable, protocol.CodeOf(appendErr))
	first := status.TerminalError
	writer.fail(&state, errors.New("later failure"))
	require.Equal(t, first, writer.Status().TerminalError)
}

func TestPruneWALSyncFailureLatchesReadiness(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	require.NoError(t, WriteSnapshot(t.Context(), dir, 1, emptySnapshotSource{}))
	file, err := os.CreateTemp(dir, "wal-sync")
	require.NoError(t, err)
	require.NoError(t, file.Close())
	writer := &Writer{config: WriterConfig{Dir: dir}}
	writer.lastLSN.Store(1)
	state := writerState{file: file}
	require.ErrorIs(t, writer.prune(&state, 1), os.ErrClosed)
	status := writer.Status()
	require.False(t, status.Ready)
	require.True(t, status.Degraded)
	require.ErrorIs(t, status.TerminalError, os.ErrClosed)
	require.NoError(t, VerifySnapshot(dir, 1))
}

type emptySnapshotSource struct{}

func (emptySnapshotSource) Range(func(table, key, value string) bool) {}

func TestLastSyncTracksPolicyFsyncs(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		policy SyncPolicy
		synced bool
	}{
		{SyncAlways, true},
		// SyncNo acknowledges without an fsync; only a snapshot's prune sets a sync time (TestLastSyncIncludesPruneSync).
		{SyncNo, false},
	} {
		t.Run(string(test.policy), func(t *testing.T) {
			t.Parallel()
			writer, err := OpenWriter(WriterConfig{Dir: t.TempDir(), Sync: test.policy, SegmentSize: 1 << 20}, 0)
			require.NoError(t, err)
			require.True(t, writer.Status().LastSync.IsZero(), "no sync has happened yet")
			_, err = writer.Append(t.Context(), CommandSet, []string{"t", "k", "v"})
			require.NoError(t, err)
			require.Equal(t, test.synced, !writer.Status().LastSync.IsZero())
			require.NoError(t, writer.Close())
		})
	}
}

type emptySource struct{}

func (emptySource) Range(func(table, key, value string) bool) {}

// TestLastSyncIncludesPruneSync covers the fsync a snapshot's prune performs: it advances LastSync under every policy,
// SyncNo included, while the watermark moves only to the snapshot's LSN.
func TestLastSyncIncludesPruneSync(t *testing.T) {
	t.Parallel()
	for _, policy := range []SyncPolicy{SyncAlways, SyncNo} {
		t.Run(string(policy), func(t *testing.T) {
			t.Parallel()
			dir := t.TempDir()
			writer, err := OpenWriter(WriterConfig{Dir: dir, Sync: policy, SegmentSize: 1 << 20}, 0)
			require.NoError(t, err)
			defer func() { require.NoError(t, writer.Close()) }()
			lsn, err := writer.Append(t.Context(), CommandSet, []string{"t", "k", "v"})
			require.NoError(t, err)
			afterAppend := time.Now()

			require.NoError(t, WriteSnapshot(t.Context(), dir, lsn, emptySource{}))
			require.NoError(t, writer.Prune(t.Context(), lsn))
			status := writer.Status()
			require.False(t, status.LastSync.IsZero())
			require.False(t, status.LastSync.Before(afterAppend), "LastSync did not advance for the prune's fsync")
			require.Equal(t, lsn, status.SyncedLSN)
		})
	}
}

// TestFailedPruneSyncKeepsLastSync: a sync that fails must not look like a successful one.
func TestFailedPruneSyncKeepsLastSync(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	require.NoError(t, WriteSnapshot(t.Context(), dir, 1, emptySource{}))
	file, err := os.CreateTemp(dir, "wal-sync")
	require.NoError(t, err)
	require.NoError(t, file.Close())

	writer := &Writer{config: WriterConfig{Dir: dir}}
	writer.lastLSN.Store(1)
	state := writerState{file: file}
	require.ErrorIs(t, writer.preparePrune(&state, 1), os.ErrClosed)
	require.True(t, writer.Status().LastSync.IsZero())
}
