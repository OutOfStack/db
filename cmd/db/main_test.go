package main

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/OutOfStack/db/internal/config"
	"github.com/OutOfStack/db/internal/engine"
	"github.com/OutOfStack/db/internal/protocol"
	"github.com/OutOfStack/db/internal/replication"
	"github.com/OutOfStack/db/internal/storage"
	"github.com/OutOfStack/db/internal/wal"
)

// freeAddr returns a currently-free localhost address for a replication listener.
func freeAddr(t *testing.T) string {
	t.Helper()
	lc := net.ListenConfig{}
	listener, err := lc.Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve address: %v", err)
	}
	addr := listener.Addr().String()
	_ = listener.Close()
	return addr
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

// TestLogSupportBoundary verifies the startup warnings fire exactly when a preview feature (tiered engine,
// replication) or ephemeral mode (in-memory without WAL) is selected.
func TestLogSupportBoundary(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		change    func(*config.ServerConfig)
		preview   bool
		ephemeral bool
	}{
		{
			name:      "default in-memory without WAL is ephemeral",
			change:    func(_ *config.ServerConfig) {},
			ephemeral: true,
		},
		{
			name:   "in-memory with WAL is fully supported",
			change: func(c *config.ServerConfig) { c.WAL.Enabled = true },
		},
		{
			name:    "tiered engine is preview, not ephemeral",
			change:  func(c *config.ServerConfig) { c.Engine.Type = engine.TypeTiered },
			preview: true,
		},
		{
			name: "replication is preview",
			change: func(c *config.ServerConfig) {
				c.WAL.Enabled = true
				c.Replication.Role = config.RoleStandby
			},
			preview: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			cfg := config.DefaultServerConfig()
			tt.change(cfg)

			var buf bytes.Buffer
			logSupportBoundary(cfg, slog.New(slog.NewTextHandler(&buf, nil)))

			out := buf.String()
			if got := strings.Contains(out, "Preview"); got != tt.preview {
				t.Errorf("log output %q: preview warning = %v, want %v", out, got, tt.preview)
			}
			if got := strings.Contains(out, "Ephemeral"); got != tt.ephemeral {
				t.Errorf("log output %q: ephemeral warning = %v, want %v", out, got, tt.ephemeral)
			}
		})
	}
}

// TestPromoteServesReplication promotes a standby and verifies it both accepts writes and serves replication to a
// downstream standby.
func TestPromoteServesReplication(t *testing.T) {
	t.Parallel()
	cfg := config.DefaultServerConfig()
	cfg.WAL.Enabled = true
	cfg.WAL.DataDir = t.TempDir()
	cfg.WAL.Sync = wal.SyncAlways
	cfg.WAL.SegmentSizeMB = 1
	cfg.Replication.Role = config.RoleStandby
	cfg.Replication.MasterAddress = "127.0.0.1:1" // unreachable; loop just retries
	cfg.Replication.ListenAddress = freeAddr(t)
	cfg.Replication.ReconnectBackoff = time.Hour
	logger := slog.New(slog.DiscardHandler)

	dbEngine, writer, _, err := recoverPersistence(cfg, logger)
	if err != nil {
		t.Fatalf("recoverPersistence() error = %v", err)
	}
	defer func() { _ = writer.Close() }()
	store := storage.New(dbEngine, storage.WithWAL(writer), storage.WithReadOnly(true))
	repl, err := setupReplication(cfg, logger, store, writer)
	if err != nil {
		t.Fatalf("setupReplication() error = %v", err)
	}
	repl.standby.Start(t.Context())
	defer stopReplication(logger, repl)

	if _, err = store.Execute(t.Context(), "SET", []string{"t", "k", "v"}); !errors.Is(err, storage.ErrReadOnly) {
		t.Fatalf("SET on standby error = %v, want ErrReadOnly", err)
	}
	if _, err = repl.admin.Promote(t.Context()); err != nil {
		t.Fatalf("Promote() error = %v", err)
	}
	if store.ReadOnly() {
		t.Fatal("store still read-only after promotion")
	}
	if _, err = store.Execute(t.Context(), "SET", []string{"t", "k", "v1"}); err != nil {
		t.Fatalf("SET after promotion error = %v", err)
	}

	// A downstream standby can be re-aimed at the promoted node and replicates.
	downstreamDir := t.TempDir()
	dsEngine := engine.New()
	dsWriter, err := wal.OpenWriter(wal.WriterConfig{Dir: downstreamDir, Sync: wal.SyncAlways, SegmentSize: 1 << 20}, 0)
	if err != nil {
		t.Fatalf("downstream OpenWriter() error = %v", err)
	}
	defer func() { _ = dsWriter.Close() }()
	dsStore := storage.New(dsEngine, storage.WithWAL(dsWriter))
	ds := replication.NewStandby(cfg.Replication.ListenAddress, dsStore, downstreamDir, 0, 10*time.Millisecond, logger)
	ds.Start(t.Context())
	defer ds.Stop()

	waitFor(t, "downstream standby to replicate from promoted node", func() bool {
		v, gErr := dsEngine.Get(context.Background(), "t", "k")
		return gErr == nil && stored(v) == "v1"
	})
}

func TestRecoverPersistenceSnapshotAndWALTail(t *testing.T) {
	t.Parallel()
	cfg := config.DefaultServerConfig()
	cfg.WAL.Enabled = true
	cfg.WAL.DataDir = t.TempDir()
	cfg.WAL.Sync = wal.SyncAlways
	cfg.WAL.SegmentSizeMB = 1
	cfg.WAL.SnapshotInterval = time.Minute
	logger := slog.New(slog.DiscardHandler)

	dbEngine, writer, _, err := recoverPersistence(cfg, logger)
	if err != nil {
		t.Fatalf("initial recoverPersistence() error = %v", err)
	}
	store := storage.New(dbEngine, storage.WithWAL(writer))
	if _, err = store.Execute(t.Context(), "SET", []string{"users", "a", "one"}); err != nil {
		t.Fatal(err)
	}
	if _, err = createSnapshot(t.Context(), cfg.WAL.DataDir, store); err != nil {
		t.Fatal(err)
	}
	if _, err = store.Execute(t.Context(), "SET", []string{"users", "b", "two"}); err != nil {
		t.Fatal(err)
	}
	if _, err = store.Execute(t.Context(), "DEL", []string{"users", "a"}); err != nil {
		t.Fatal(err)
	}
	if err = writer.Close(); err != nil {
		t.Fatal(err)
	}

	recovered, recoveredWriter, snapshotLSN, err := recoverPersistence(cfg, logger)
	if err != nil {
		t.Fatalf("recoverPersistence() error = %v", err)
	}
	defer func() { _ = recoveredWriter.Close() }()
	if snapshotLSN != 1 {
		t.Fatalf("snapshot LSN = %d, want 1", snapshotLSN)
	}
	if _, err = recovered.Get(t.Context(), "users", "a"); !errors.Is(err, engine.ErrNotFound) {
		t.Fatalf("deleted key error = %v, want ErrNotFound", err)
	}
	value, err := recovered.Get(t.Context(), "users", "b")
	if err != nil || stored(value) != "two" {
		t.Fatalf("recovered value = %q, %v; want two, nil", value, err)
	}
}

func stored(value string) string {
	return protocol.Render(protocol.Decode(value))
}

func TestRecoverPersistenceTypedValues(t *testing.T) {
	t.Parallel()
	cfg := config.DefaultServerConfig()
	cfg.WAL.Enabled = true
	cfg.WAL.DataDir = t.TempDir()
	cfg.WAL.Sync = wal.SyncAlways
	cfg.WAL.SegmentSizeMB = 1
	cfg.WAL.SnapshotInterval = time.Minute
	logger := slog.New(slog.DiscardHandler)

	dbEngine, writer, _, err := recoverPersistence(cfg, logger)
	if err != nil {
		t.Fatalf("initial recoverPersistence() error = %v", err)
	}
	store := storage.New(dbEngine, storage.WithWAL(writer))

	commands := [][]string{
		{"SET", "t", "conf", `{"a":[1,2],"b":"x"}`},
		{"SET", "t", "name", "vlad"},
		{"INCR", "t", "hits", "5"},
		{"INCR", "t", "hits", "0.5"},
		{"APPEND", "t", "log", "first"},
		{"APPEND", "t", "log", "2"},
		{"HSET", "t", "user", "name", "vlad"},
		{"HSET", "t", "user", "age", "42"},
		{"SET", "t", "big", "9223372036854775807"},
		// Both fail after they are already durable, so recovery has to replay them as the no-ops they were instead of
		// refusing to start.
		{"INCR", "t", "name"}, // wrong type
		{"INCR", "t", "big"},  // int64 overflow
	}
	for _, command := range commands {
		if _, err = store.Execute(t.Context(), command[0], command[1:]); err != nil && command[0] != "INCR" {
			t.Fatalf("%v: %v", command, err)
		}
	}
	if _, err = createSnapshot(t.Context(), cfg.WAL.DataDir, store); err != nil {
		t.Fatal(err)
	}
	if _, err = store.Execute(t.Context(), "APPEND", []string{"t", "log", "true"}); err != nil {
		t.Fatal(err)
	}
	if err = writer.Close(); err != nil {
		t.Fatal(err)
	}

	recovered, recoveredWriter, _, err := recoverPersistence(cfg, logger)
	if err != nil {
		t.Fatalf("recoverPersistence() error = %v", err)
	}
	defer func() { _ = recoveredWriter.Close() }()

	want := map[string]string{
		"conf": `{"a":[1,2],"b":"x"}`,
		"name": "vlad",
		"hits": "5.5",
		"log":  `["first",2,true]`,
		"user": `{"age":42,"name":"vlad"}`,
		"big":  "9223372036854775807",
	}
	for key, wantValue := range want {
		value, gErr := recovered.Get(t.Context(), "t", key)
		if gErr != nil {
			t.Fatalf("recovered Get(%s) error = %v", key, gErr)
		}
		if got := stored(value); got != wantValue {
			t.Errorf("recovered t/%s = %s, want %s", key, got, wantValue)
		}
	}
}
