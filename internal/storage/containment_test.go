package storage_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

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

// TestStorage_PanickingApplyReleasesTheGate covers the apply gate under a recovered panic: a mutation already logged
// under the next LSN, and so already past the fence check, still completes instead of waiting forever behind the one
// that panicked and holding up shutdown.
func TestStorage_PanickingApplyReleasesTheGate(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	mockEngine := mocks.NewMockEngine(gomock.NewController(t))
	queued := make(chan struct{})
	log := &fakeWAL{append: func(_ context.Context, _ string, args []string) (uint64, error) {
		if args[1] == "next" {
			close(queued)
			return 2, nil
		}
		return 1, nil
	}}
	store := storage.New(mockEngine, storage.WithWAL(log))

	mockEngine.EXPECT().Set(gomock.Any(), "t", "next", gomock.Any()).Return(nil)
	done := make(chan error, 1)
	go func() {
		_, err := store.Execute(ctx, "SET", []string{"t", "next", "v"})
		done <- err
	}()
	<-queued

	mockEngine.EXPECT().Set(gomock.Any(), "t", "boom", gomock.Any()).DoAndReturn(
		func(context.Context, string, string, string) error { panic("engine invariant violated") })
	require.Panics(t, func() { _, _ = store.Execute(ctx, "SET", []string{"t", "boom", "v"}) })

	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("mutation queued behind a panicking apply never ran")
	}
}

// TestStorage_PanickingMutationFencesBeforeSnapshot covers a snapshot racing a mutation that panics. The panicked record
// is in the WAL but not (or only partly) in the engine, so a snapshot taken at that LSN would persist the wrong state
// and prune the record that could repair it. The snapshot below passes its first fence check while the mutation still
// holds the state lock, and must still refuse once it gets the lock.
func TestStorage_PanickingMutationFencesBeforeSnapshot(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	mockEngine := mocks.NewMockEngine(gomock.NewController(t))
	log := &fakeWAL{append: func(context.Context, string, []string) (uint64, error) { return 1, nil }}
	store := storage.New(mockEngine, storage.WithWAL(log))

	snapshotDone := make(chan error, 1)
	// Reached only when the snapshot captures state; the write callback then reports the failure.
	mockEngine.EXPECT().Range(gomock.Any()).AnyTimes()
	mockEngine.EXPECT().Set(gomock.Any(), "t", "boom", gomock.Any()).DoAndReturn(
		func(context.Context, string, string, string) error {
			go func() {
				snapshotDone <- store.Snapshot(ctx, func(context.Context, uint64, storage.SnapshotSource) error {
					t.Error("snapshot captured the state a panicking mutation left behind")
					return nil
				})
			}()
			// Give the snapshot time to pass its first fence check and block on the state lock this mutation holds.
			time.Sleep(50 * time.Millisecond)
			panic("engine invariant violated")
		})
	require.Panics(t, func() { _, _ = store.Execute(ctx, "SET", []string{"t", "boom", "v"}) })

	select {
	case err := <-snapshotDone:
		require.ErrorIs(t, err, storage.ErrTerminal)
	case <-time.After(time.Second):
		t.Fatal("snapshot did not return")
	}
	require.ErrorIs(t, store.Terminal(), storage.ErrTerminal)
	require.Zero(t, log.pruned, "WAL was pruned past the panicked record")
	_, err := store.Execute(ctx, "GET", []string{"t", "boom"})
	require.ErrorIs(t, err, storage.ErrTerminal)
}
