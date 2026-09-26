package main

import (
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/OutOfStack/db/internal/config"
	"github.com/OutOfStack/db/internal/datadir"
	"github.com/OutOfStack/db/internal/engine"
	"github.com/OutOfStack/db/internal/engine/tiered"
	"github.com/OutOfStack/db/internal/protocol"
	"github.com/OutOfStack/db/internal/status"
	"github.com/OutOfStack/db/internal/storage"
	"github.com/OutOfStack/db/internal/wal"
	"github.com/stretchr/testify/require"
)

// TestPrepareDataDirChecksManifest covers the restore guard: a directory's manifest has to match the configured engine
// and this build's formats, and it is checked before recovery opens anything — including when the manifest is the only
// file there, which is what a restore into an otherwise empty directory can look like.
func TestPrepareDataDirChecksManifest(t *testing.T) {
	t.Parallel()

	wrongFormat := datadir.NewManifest(engine.TypeInMemory, wal.SyncAlways, "v0.9.0")
	wrongFormat.Formats[datadir.FormatSnapshot] = wal.SnapshotFormatVersion - 1

	tests := []struct {
		name     string
		engine   string
		manifest *datadir.Manifest
		raw      string
		wantErr  string
	}{
		{
			name: "matching manifest", engine: engine.TypeInMemory,
			manifest: new(datadir.NewManifest(engine.TypeInMemory, wal.SyncEverySec, "v1.0.0")),
		},
		{
			name: "tiered manifest under in_memory", engine: engine.TypeInMemory,
			manifest: new(datadir.NewManifest(engine.TypeTiered, wal.SyncEverySec, "v1.0.0")),
			wantErr:  "holds tiered engine files, but the configuration selects in_memory",
		},
		{
			name: "in_memory manifest under tiered", engine: engine.TypeTiered,
			manifest: new(datadir.NewManifest(engine.TypeInMemory, wal.SyncEverySec, "v1.0.0")),
			wantErr:  "holds in_memory engine files, but the configuration selects tiered",
		},
		{name: "format mismatch", engine: engine.TypeInMemory, manifest: &wrongFormat, wantErr: "do not match"},
		{name: "unparsable manifest", engine: engine.TypeInMemory, raw: "{", wantErr: "parse"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			dir := t.TempDir()
			if test.manifest != nil {
				require.NoError(t, datadir.WriteManifest(dir, *test.manifest))
			} else {
				require.NoError(t, os.WriteFile(filepath.Join(dir, datadir.ManifestName), []byte(test.raw), 0o600))
			}
			cfg := config.DefaultServerConfig()
			cfg.Engine.Type = test.engine
			cfg.Engine.DataDir = dir
			cfg.WAL.Enabled = test.engine == engine.TypeInMemory
			cfg.WAL.DataDir = dir

			lock, err := prepareDataDir(cfg, false)
			if test.wantErr == "" {
				require.NoError(t, err)
				require.NoError(t, lock.Close())
				return
			}
			require.ErrorContains(t, err, test.wantErr)
			// The refusal released the lock, so the operator can fix the configuration and start again.
			lock, err = datadir.Acquire(dir)
			require.NoError(t, err)
			require.NoError(t, lock.Close())
		})
	}
}

// statusOf runs STATUS through the same compute a server builds and returns the reply as a map.
func statusOf(t *testing.T, cfg *config.ServerConfig, store *storage.Storage, repl *replicationRuntime) map[string]string {
	t.Helper()
	reply := newCompute(cfg, slog.New(slog.DiscardHandler), store, repl).Handle(t.Context(), "STATUS", nil)
	require.Equal(t, protocol.ReplyArray, reply.Kind, "STATUS reply: %+v", reply)
	values := make(map[string]string, len(reply.Array)/2)
	for i := 0; i+1 < len(reply.Array); i += 2 {
		values[reply.Array[i].Value] = reply.Array[i+1].Value
	}
	return values
}

func TestStatusReportsEphemeralMode(t *testing.T) {
	t.Parallel()
	cfg := config.DefaultServerConfig()
	got := statusOf(t, cfg, storage.New(engine.New()), nil)
	require.Equal(t, "true", got["ready"])
	require.Equal(t, status.StateOK, got["state"])
	require.Equal(t, status.DurabilityEphemeral, got["durability"])
	require.Equal(t, engine.TypeInMemory, got["engine"])
	require.Equal(t, status.RoleStandalone, got["role"])
	require.Equal(t, "0", got["applied_lsn"])
	require.Empty(t, got["last_sync"])
}

func TestStatusReportsDurableProgressAndFence(t *testing.T) {
	t.Parallel()
	cfg := shutdownTestConfig("127.0.0.1:0", t.TempDir())
	logger := slog.New(slog.DiscardHandler)
	dbEngine, writer, snapshotLSN, _, err := recoverPersistence(cfg, logger)
	require.NoError(t, err)
	defer func() { require.NoError(t, writer.Close()) }()
	store := storage.New(dbEngine, storageOptions(cfg, writer, snapshotLSN)...)
	comp := newCompute(cfg, logger, store, nil)

	require.Equal(t, protocol.SimpleString("OK"), comp.Handle(t.Context(), "SET", []string{"users", "name", "vlad"}))
	_, err = createSnapshot(t.Context(), cfg.WAL.DataDir, store)
	require.NoError(t, err)

	got := statusOf(t, cfg, store, nil)
	require.Equal(t, "always", got["durability"])
	require.Equal(t, "1", got["applied_lsn"])
	require.Equal(t, "1", got["synced_lsn"])
	require.Equal(t, "1", got["snapshot_lsn"])
	for _, field := range []string{"last_sync", "last_snapshot"} {
		_, err = time.Parse(time.RFC3339, got[field])
		require.NoError(t, err, "%s = %q", field, got[field])
	}

	// A fenced node refuses every command, and STATUS is how the operator sees why.
	store.Fence(errors.New("resync failed part-way"))
	require.Equal(t, protocol.CodeUnavailable, comp.Handle(t.Context(), "GET", []string{"users", "name"}).Code)
	require.Equal(t, protocol.SimpleString("PONG"), comp.Handle(t.Context(), "PING", nil))
	got = statusOf(t, cfg, store, nil)
	require.Equal(t, "false", got["ready"])
	require.Equal(t, status.StateTerminal, got["state"])
	require.Contains(t, got["error"], "resync failed part-way")
}

func TestStatusFollowsPromotion(t *testing.T) {
	t.Parallel()
	cfg := shutdownTestConfig("127.0.0.1:0", t.TempDir())
	cfg.Replication.Role = config.RoleStandby
	cfg.Replication.MasterAddress = "127.0.0.1:1" // unreachable; the standby only retries
	cfg.Replication.ReconnectBackoff = time.Hour
	logger := slog.New(slog.DiscardHandler)

	dbEngine, writer, _, _, err := recoverPersistence(cfg, logger)
	require.NoError(t, err)
	defer func() { _ = writer.Close() }()
	store := storage.New(dbEngine, storageOptions(cfg, writer, 0)...)
	repl, err := setupReplication(cfg, logger, store, writer, "")
	require.NoError(t, err)
	startReplication(t.Context(), logger, repl)
	defer func() { _ = stopReplication(repl) }()

	require.Equal(t, "standby", statusOf(t, cfg, store, repl)["role"])
	_, err = repl.admin.Promote(t.Context())
	require.NoError(t, err)
	require.Equal(t, "master", statusOf(t, cfg, store, repl)["role"])
}

func TestStatusReportsTieredEngine(t *testing.T) {
	t.Parallel()
	cfg := config.DefaultServerConfig()
	cfg.Engine.Type = engine.TypeTiered
	cfg.Engine.DataDir = t.TempDir()
	cfg.Engine.Sync = wal.SyncAlways
	dbEngine, err := tiered.Open(tieredConfig(cfg.Engine), slog.New(slog.DiscardHandler))
	require.NoError(t, err)
	defer func() { require.NoError(t, dbEngine.Close()) }()
	store := storage.New(dbEngine, storageOptions(cfg, nil, 0)...)

	comp := newCompute(cfg, slog.New(slog.DiscardHandler), store, nil)
	require.Equal(t, protocol.SimpleString("OK"), comp.Handle(t.Context(), "SET", []string{"users", "name", "vlad"}))
	got := statusOf(t, cfg, store, nil)
	require.Equal(t, engine.TypeTiered, got["engine"])
	require.Equal(t, "always", got["durability"])
	require.Equal(t, "0", got["applied_lsn"], "the tiered engine keeps no log sequence")
	require.NotEmpty(t, got["last_sync"])
}

// TestServerBoundsListingsByMessageSize checks the server wires its message-size limit into the listing bound.
func TestServerBoundsListingsByMessageSize(t *testing.T) {
	t.Parallel()
	cfg := config.DefaultServerConfig()
	cfg.Network.MaxMessageSizeKB = 1
	store := storage.New(engine.New(), storageOptions(cfg, nil, 0)...)
	comp := newCompute(cfg, slog.New(slog.DiscardHandler), store, nil)
	for i := range 100 {
		key := "key-" + string(rune('a'+i%26)) + string(rune('a'+i/26))
		require.Equal(t, protocol.SimpleString("OK"), comp.Handle(t.Context(), "SET", []string{"users", key, "v"}))
	}
	require.Equal(t, protocol.CodeTooLarge, comp.Handle(t.Context(), "KEYS", []string{"users"}).Code)
	require.Equal(t, protocol.ReplyArray, comp.Handle(t.Context(), "TABLES", nil).Kind)
}
