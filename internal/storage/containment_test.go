package storage_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/OutOfStack/db/internal/engine"
	"github.com/OutOfStack/db/internal/storage"
	mocks "github.com/OutOfStack/db/internal/storage/mocks"
	"github.com/OutOfStack/db/internal/wal"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

// healthEngine is a mock engine that also reports its own status, like the tiered engine does.
type healthEngine struct {
	*mocks.MockEngine
	status wal.Status
}

func (h *healthEngine) Status() wal.Status { return h.status }

func TestStorage_FenceRefusesEveryCommand(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	mockEngine := mocks.NewMockEngine(gomock.NewController(t))
	store := storage.New(mockEngine)
	require.NoError(t, store.Terminal())
	require.True(t, store.Status().Ready)

	first := errors.New("histories diverged")
	store.Fence(first)
	store.Fence(errors.New("later cause is ignored"))
	for _, cmd := range [][]string{{"GET", "t", "k"}, {"SET", "t", "k", "v"}, {"TABLES"}, {"KEYS", "t"}} {
		_, err := store.Execute(ctx, cmd[0], cmd[1:])
		require.ErrorIs(t, err, storage.ErrTerminal, cmd[0])
		require.ErrorIs(t, err, first, cmd[0])
	}
	status := store.Status()
	require.False(t, status.Ready)
	require.True(t, status.Degraded)
	require.ErrorIs(t, status.TerminalError, first)
}

func TestStorage_StatusFoldsInEngineHealth(t *testing.T) {
	t.Parallel()
	eng := &healthEngine{MockEngine: mocks.NewMockEngine(gomock.NewController(t)), status: wal.Status{Ready: true}}
	store := storage.New(eng)
	require.True(t, store.Status().Ready)

	terminal := errors.New("fsync failed")
	eng.status = wal.Status{Ready: false, Degraded: true, TerminalError: terminal}
	status := store.Status()
	require.False(t, status.Ready)
	require.True(t, status.Degraded)
	require.ErrorIs(t, status.TerminalError, terminal)

	maintenance := errors.New("compaction failed")
	eng.status = wal.Status{Ready: true, Degraded: true, MaintenanceError: maintenance}
	status = store.Status()
	require.True(t, status.Ready)
	require.True(t, status.Degraded)
	require.ErrorIs(t, status.MaintenanceError, maintenance)
}

type resettingWAL struct {
	fakeWAL
	resetErr error
}

func (r *resettingWAL) Reset(_ context.Context, lsn uint64) error {
	if r.resetErr != nil {
		return r.resetErr
	}
	r.last = lsn
	return nil
}

func TestStorage_FailedResetToSnapshotFences(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	mockEngine := mocks.NewMockEngine(gomock.NewController(t))
	log := &resettingWAL{resetErr: errors.New("reset refused")}
	store := storage.New(mockEngine, storage.WithWAL(log), storage.WithReadOnly(true))
	err := store.ResetToSnapshot(ctx, t.TempDir(), 7, []engine.Entry{{Table: "t", Key: "k", Value: "v"}})
	require.ErrorIs(t, err, log.resetErr)
	require.ErrorIs(t, store.Terminal(), storage.ErrTerminal)
	require.ErrorContains(t, store.Terminal(), "resync at LSN 7 failed part-way")
	_, err = store.Execute(ctx, "GET", []string{"t", "k"})
	require.ErrorIs(t, err, storage.ErrTerminal)
}

// TestStorage_NoSnapshotAfterFailedResync: the WAL reset to the master's LSN but the snapshot could not be published,
// so the engine still holds the older state. Maintenance must not write that state under the new LSN.
func TestStorage_NoSnapshotAfterFailedResync(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	mockEngine := mocks.NewMockEngine(gomock.NewController(t))
	log := &resettingWAL{}
	store := storage.New(mockEngine, storage.WithWAL(log), storage.WithReadOnly(true))
	notADir := filepath.Join(t.TempDir(), "file")
	require.NoError(t, os.WriteFile(notADir, nil, 0o600))
	err := store.ResetToSnapshot(ctx, notADir, 9, []engine.Entry{{Table: "t", Key: "k", Value: "v"}})
	require.Error(t, err)
	require.Equal(t, uint64(9), log.LastLSN(), "the WAL was reset before publication failed")
	require.ErrorIs(t, store.Terminal(), storage.ErrTerminal)

	written := false
	err = store.Snapshot(ctx, func(context.Context, uint64, storage.SnapshotSource) error {
		written = true
		return nil
	})
	require.ErrorIs(t, err, storage.ErrTerminal)
	require.False(t, written, "maintenance captured engine state under a resync LSN it does not hold")
	require.Zero(t, log.pruned)
}

func TestStorage_FenceBlocksSnapshot(t *testing.T) {
	t.Parallel()
	mockEngine := mocks.NewMockEngine(gomock.NewController(t))
	store := storage.New(mockEngine, storage.WithWAL(&fakeWAL{last: 3}))
	store.Fence(errors.New("diverged"))
	err := store.Snapshot(t.Context(), func(context.Context, uint64, storage.SnapshotSource) error {
		t.Fatal("snapshot written on a fenced storage")
		return nil
	})
	require.ErrorIs(t, err, storage.ErrTerminal)
}
