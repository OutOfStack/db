package wal

import (
	"errors"
	"os"
	"testing"

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
