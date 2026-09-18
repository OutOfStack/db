package main

import (
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"

	"github.com/OutOfStack/db/internal/config"
	"github.com/OutOfStack/db/internal/engine"
	"github.com/OutOfStack/db/internal/protocol"
	"github.com/OutOfStack/db/internal/replication"
	"github.com/OutOfStack/db/internal/storage"
	"github.com/OutOfStack/db/internal/wal"
	"github.com/stretchr/testify/require"
)

func TestUnverifiedHistoryReason(t *testing.T) {
	t.Parallel()
	cfg := config.DefaultServerConfig()
	for _, policy := range []wal.SyncPolicy{wal.SyncAlways, wal.SyncEverySec, wal.SyncNo} {
		cfg.WAL.Sync = policy
		require.Empty(t, unverifiedHistoryReason(cfg, false), policy)
		reason := unverifiedHistoryReason(cfg, true)
		if policy == wal.SyncAlways {
			require.Empty(t, reason, "a torn tail under always was never acknowledged or streamed")
		} else {
			require.Contains(t, reason, string(policy))
		}
	}
}

// TestPromoteRefusedWhileTerminal: a fenced standby must not become the writable master, and the status command
// reports the terminal state so an operator can see why.
func TestPromoteRefusedWhileTerminal(t *testing.T) {
	t.Parallel()
	cfg := config.DefaultServerConfig()
	cfg.WAL.Enabled = true
	cfg.WAL.DataDir = t.TempDir()
	cfg.WAL.Sync = wal.SyncAlways
	cfg.WAL.SegmentSizeMB = 1
	cfg.Replication.Role = config.RoleStandby
	cfg.Replication.MasterAddress = "127.0.0.1:1" // unreachable; loop just retries
	cfg.Replication.ReconnectBackoff = time.Hour
	logger := slog.New(slog.DiscardHandler)

	dbEngine, writer, _, _, err := recoverPersistence(cfg, logger)
	require.NoError(t, err)
	defer func() { _ = writer.Close() }()
	store := storage.New(dbEngine, storage.WithWAL(writer), storage.WithReadOnly(true))
	repl, err := setupReplication(cfg, logger, store, writer, "")
	require.NoError(t, err)
	startReplication(t.Context(), logger, repl)
	defer func() { _ = stopReplication(repl) }()

	reply, err := repl.admin.Status(t.Context())
	require.NoError(t, err)
	require.Equal(t, "ok", valueAfter(t, reply, "state"))

	store.Fence(errors.New("resync failed part-way"))
	_, err = repl.admin.Promote(t.Context())
	require.ErrorContains(t, err, "refusing to promote")
	require.ErrorContains(t, err, "resync failed part-way")
	require.True(t, store.ReadOnly())

	reply, err = repl.admin.Status(t.Context())
	require.NoError(t, err)
	require.Equal(t, "terminal", valueAfter(t, reply, "state"))
	require.Contains(t, valueAfter(t, reply, "error"), "resync failed part-way")
}

// valueAfter returns the value following key in a flat key/value array reply.
func valueAfter(t *testing.T, reply protocol.Reply, key string) string {
	t.Helper()
	for i := 0; i+1 < len(reply.Array); i += 2 {
		if reply.Array[i].Value == key {
			return reply.Array[i+1].Value
		}
	}
	t.Fatalf("key %q not in %v", key, reply.Array)
	return ""
}

// stallingApplier holds a resync open until the standby is stopped, then reports the failure, standing in for a
// storage cut off while replacing its state.
type stallingApplier struct {
	*storage.Storage
	started chan struct{}
}

func (a *stallingApplier) ResetToSnapshot(ctx context.Context, _ string, _ uint64, _ []engine.Entry) error {
	close(a.started)
	<-ctx.Done()
	return ctx.Err()
}

func (a *stallingApplier) Fence(err error) { a.Storage.Fence(err) }

// TestPromoteRefusedWhenStopMakesStandbyTerminal: PROMOTE stops replication, which can cut off a resync in progress
// and make the standby terminal only then. The promotion must notice and refuse rather than crown that state.
func TestPromoteRefusedWhenStopMakesStandbyTerminal(t *testing.T) {
	t.Parallel()
	logger := slog.New(slog.DiscardHandler)
	masterCfg := config.DefaultServerConfig()
	masterCfg.WAL.Enabled = true
	masterCfg.WAL.DataDir = t.TempDir()
	masterCfg.WAL.Sync = wal.SyncAlways
	masterCfg.WAL.SegmentSizeMB = 1
	masterEngine, masterWriter, _, _, err := recoverPersistence(masterCfg, logger)
	require.NoError(t, err)
	defer func() { _ = masterWriter.Close() }()
	masterStore := storage.New(masterEngine, storage.WithWAL(masterWriter))
	_, err = masterStore.Execute(t.Context(), "SET", []string{"t", "k1", "v1"})
	require.NoError(t, err)
	_, err = createSnapshot(t.Context(), masterCfg.WAL.DataDir, masterStore) // prunes, so a standby must resync
	require.NoError(t, err)
	master, err := replication.NewMaster("127.0.0.1:0", masterWriter, masterCfg.WAL.DataDir, logger)
	require.NoError(t, err)
	go master.Serve(t.Context())
	defer func() { _ = master.Close() }()

	standbyDir := t.TempDir()
	standbyWriter, err := wal.OpenWriter(wal.WriterConfig{Dir: standbyDir, Sync: wal.SyncAlways, SegmentSize: 1 << 20}, 0)
	require.NoError(t, err)
	defer func() { _ = standbyWriter.Close() }()
	store := storage.New(engine.New(), storage.WithWAL(standbyWriter), storage.WithReadOnly(true))
	applier := &stallingApplier{Storage: store, started: make(chan struct{})}
	standby := replication.NewStandby(master.Addr().String(), applier, standbyDir, 0, time.Hour, logger)
	standby.Start(t.Context())
	admin := &replicationAdmin{store: store, writer: standbyWriter, standby: standby, logger: logger, role: config.RoleStandby}
	defer func() { _ = admin.close() }()
	select {
	case <-applier.started:
	case <-time.After(5 * time.Second):
		t.Fatal("resync never started")
	}

	_, err = admin.Promote(t.Context())
	require.ErrorContains(t, err, "refusing to promote")
	require.ErrorIs(t, err, replication.ErrTerminal)
	require.True(t, store.ReadOnly(), "promotion changed the store despite the terminal standby")
	require.Equal(t, config.RoleStandby, admin.role)
}
