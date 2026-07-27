package replication_test

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/OutOfStack/db/internal/engine"
	"github.com/OutOfStack/db/internal/replication"
	"github.com/OutOfStack/db/internal/storage"
	"github.com/OutOfStack/db/internal/wal"
)

// node bundles the persistence and engine for one in-process server.
type node struct {
	dir    string
	engine *engine.Engine
	writer *wal.Writer
	store  *storage.Storage
}

func newNode(t *testing.T, dir string) *node {
	t.Helper()
	writer, err := wal.OpenWriter(wal.WriterConfig{Dir: dir, Sync: wal.SyncAlways, SegmentSize: 4 << 10}, 0)
	if err != nil {
		t.Fatalf("OpenWriter: %v", err)
	}
	eng := engine.New()
	t.Cleanup(func() { _ = writer.Close() })
	return &node{
		dir:    dir,
		engine: eng,
		writer: writer,
		store:  storage.New(eng, storage.WithWAL(writer)),
	}
}

// reopen recovers a node from its data directory, returning the recovered
// applied LSN. It simulates a restart: load the snapshot, replay the WAL tail.
func reopenNode(t *testing.T, dir string) (*node, uint64) {
	t.Helper()
	eng := engine.New()
	var entries []engine.Entry
	snapshotLSN, err := wal.LoadLatestSnapshot(dir, func(table, key, value string) error {
		entries = append(entries, engine.Entry{Table: table, Key: key, Value: value})
		return nil
	})
	if err != nil {
		t.Fatalf("LoadLatestSnapshot: %v", err)
	}
	eng.Load(context.Background(), entries)
	lastLSN, err := wal.NewReader(dir, nil).Replay(snapshotLSN, func(record wal.Record) error {
		switch record.Command {
		case wal.CommandSet:
			return eng.Set(context.Background(), record.Args[0], record.Args[1], record.Args[2])
		case wal.CommandDel:
			_ = eng.Del(context.Background(), record.Args[0], record.Args[1])
			return nil
		default:
			return nil
		}
	})
	if err != nil {
		t.Fatalf("Replay: %v", err)
	}
	writer, err := wal.OpenWriter(wal.WriterConfig{Dir: dir, Sync: wal.SyncAlways, SegmentSize: 4 << 10}, lastLSN)
	if err != nil {
		t.Fatalf("OpenWriter: %v", err)
	}
	t.Cleanup(func() { _ = writer.Close() })
	return &node{
		dir:    dir,
		engine: eng,
		writer: writer,
		store:  storage.New(eng, storage.WithWAL(writer)),
	}, lastLSN
}

func set(t *testing.T, n *node, table, key, value string) {
	t.Helper()
	if _, err := n.store.Execute(context.Background(), "SET", []string{table, key, value}); err != nil {
		t.Fatalf("SET %s/%s: %v", table, key, err)
	}
}

func snapshot(t *testing.T, n *node) {
	t.Helper()
	err := n.store.Snapshot(context.Background(), func(ctx context.Context, lsn uint64, src storage.SnapshotSource) error {
		return wal.WriteSnapshot(ctx, n.dir, lsn, src)
	})
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
}

func startMaster(t *testing.T, n *node) *replication.Master {
	t.Helper()
	master, err := replication.NewMaster("127.0.0.1:0", n.writer, n.dir, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatalf("NewMaster: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	go master.Serve(ctx)
	t.Cleanup(func() {
		cancel()
		_ = master.Close()
	})
	return master
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	for range 200 {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func mustGet(t *testing.T, n *node, table, key string) string {
	t.Helper()
	value, err := n.engine.Get(context.Background(), table, key)
	if err != nil {
		t.Fatalf("standby Get %s/%s: %v", table, key, err)
	}
	return value
}

// TestReplication_StreamsWrites verifies a standby receives and applies live
// writes from the master and reports zero lag once caught up.
func TestReplication_StreamsWrites(t *testing.T) {
	t.Parallel()
	master := newNode(t, t.TempDir())
	standby := newNode(t, t.TempDir())
	m := startMaster(t, master)

	set(t, master, "users", "a", "one")
	set(t, master, "users", "b", "two")

	sb := replication.NewStandby(m.Addr().String(), standby.store, standby.dir, 0, 10*time.Millisecond, nil)
	sb.Start(context.Background())
	t.Cleanup(sb.Stop)

	waitFor(t, "standby to apply both writes", func() bool { return sb.AppliedLSN() >= 2 })
	if got := mustGet(t, standby, "users", "a"); got != "one" {
		t.Errorf("standby users/a = %q, want one", got)
	}
	if got := mustGet(t, standby, "users", "b"); got != "two" {
		t.Errorf("standby users/b = %q, want two", got)
	}

	// A write after the standby is connected streams live.
	set(t, master, "users", "c", "three")
	waitFor(t, "standby to apply live write", func() bool { return sb.AppliedLSN() >= 3 })
	if got := mustGet(t, standby, "users", "c"); got != "three" {
		t.Errorf("standby users/c = %q, want three", got)
	}
	waitFor(t, "lag to settle to zero", func() bool { return sb.Lag() == 0 })
}

// TestReplication_ResumesFromLSN verifies a restarted standby resumes from its
// persisted LSN instead of re-fetching the whole log.
func TestReplication_ResumesFromLSN(t *testing.T) {
	t.Parallel()
	master := newNode(t, t.TempDir())
	standbyDir := t.TempDir()
	standby := newNode(t, standbyDir)
	m := startMaster(t, master)

	set(t, master, "t", "k1", "v1")
	set(t, master, "t", "k2", "v2")

	sb := replication.NewStandby(m.Addr().String(), standby.store, standby.dir, 0, 10*time.Millisecond, nil)
	sb.Start(context.Background())
	waitFor(t, "initial catch-up", func() bool { return sb.AppliedLSN() >= 2 })
	sb.Stop()
	if err := standby.writer.Close(); err != nil {
		t.Fatalf("close standby writer: %v", err)
	}

	// Master advances while the standby is offline.
	set(t, master, "t", "k3", "v3")

	reopened, lastLSN := reopenNode(t, standbyDir)
	t.Cleanup(func() { _ = reopened.writer.Close() })
	if lastLSN != 2 {
		t.Fatalf("recovered standby LSN = %d, want 2", lastLSN)
	}
	sb2 := replication.NewStandby(m.Addr().String(), reopened.store, reopened.dir, lastLSN, 10*time.Millisecond, nil)
	sb2.Start(context.Background())
	t.Cleanup(sb2.Stop)

	waitFor(t, "standby to resume and apply k3", func() bool { return sb2.AppliedLSN() >= 3 })
	if got := mustGet(t, reopened, "t", "k3"); got != "v3" {
		t.Errorf("resumed standby t/k3 = %q, want v3", got)
	}
	// Earlier keys survived the restart via the standby's own WAL.
	if got := mustGet(t, reopened, "t", "k1"); got != "v1" {
		t.Errorf("resumed standby t/k1 = %q, want v1", got)
	}
}

// TestReplication_SnapshotResync verifies a standby far enough behind that the
// master has pruned its WAL bootstraps from a shipped snapshot.
func TestReplication_SnapshotResync(t *testing.T) {
	t.Parallel()
	master := newNode(t, t.TempDir())
	standby := newNode(t, t.TempDir())
	m := startMaster(t, master)

	set(t, master, "t", "k1", "v1")
	set(t, master, "t", "k2", "v2")
	set(t, master, "t", "k3", "v3")
	// Snapshot + prune removes the WAL segments below the snapshot LSN, so a
	// fresh standby cannot be served from segments alone.
	snapshot(t, master)
	set(t, master, "t", "k4", "v4")

	sb := replication.NewStandby(m.Addr().String(), standby.store, standby.dir, 0, 10*time.Millisecond, nil)
	sb.Start(context.Background())
	t.Cleanup(sb.Stop)

	waitFor(t, "standby to resync and apply k4", func() bool { return sb.AppliedLSN() >= 4 })
	for key, want := range map[string]string{"k1": "v1", "k2": "v2", "k3": "v3", "k4": "v4"} {
		if got := mustGet(t, standby, "t", key); got != want {
			t.Errorf("standby t/%s = %q, want %q", key, got, want)
		}
	}
	// The resync leaves a snapshot on the standby so it can restart standalone.
	lsn, _, ok, err := wal.LatestSnapshotInfo(standby.dir)
	if err != nil || !ok || lsn == 0 {
		t.Fatalf("standby snapshot info = %d, %v, %v; want a snapshot", lsn, ok, err)
	}
}
