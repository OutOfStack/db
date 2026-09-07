package wal

import (
	"errors"
	"os"
	"testing"

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
	require.Equal(t, uint64(3), status.SyncedLSN)
	result := make(chan writerResult, 1)
	writer.handleBatch([]writerRequest{{result: result}}, &state)
	require.ErrorIs(t, (<-result).err, os.ErrClosed)
	first := status.TerminalError
	writer.fail(&state, errors.New("later failure"))
	require.Equal(t, first, writer.Status().TerminalError)
}
