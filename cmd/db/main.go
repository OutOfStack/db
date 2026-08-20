package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/OutOfStack/db/internal/compute"
	"github.com/OutOfStack/db/internal/config"
	"github.com/OutOfStack/db/internal/datadir"
	"github.com/OutOfStack/db/internal/engine"
	"github.com/OutOfStack/db/internal/engine/tiered"
	"github.com/OutOfStack/db/internal/network"
	"github.com/OutOfStack/db/internal/parser"
	"github.com/OutOfStack/db/internal/protocol"
	"github.com/OutOfStack/db/internal/storage"
	"github.com/OutOfStack/db/internal/wal"
)

func main() {
	os.Exit(execute())
}

// execute runs the server and returns a process exit code. It is separate from main so deferred cleanup (e.g. closing
// the log file) runs before os.Exit.
func execute() int {
	var configPath string
	var allowEphemeralOverData bool
	flag.StringVar(&configPath, "config", "", "Path to configuration file")
	flag.BoolVar(&allowEphemeralOverData, "allow-ephemeral-over-data", false,
		"Allow ephemeral startup when durable database files already exist")
	flag.Parse()

	cfg, err := config.LoadServerConfig(configPath)
	if err != nil {
		log.Printf("Failed to load configuration: %v\n", err)
		return 1
	}
	logger, closeLog, err := newLogger(cfg.Logging)
	if err != nil {
		log.Printf("Failed to configure logging: %v\n", err)
		return 1
	}
	runErr := run(cfg, logger, allowEphemeralOverData)
	if runErr != nil {
		logger.Error("Server stopped", "error", runErr)
	}
	closeErr := closeLog()
	if closeErr != nil {
		log.Printf("Failed to close log file: %v", closeErr)
	}
	return processExitCode(runErr, closeErr)
}

func processExitCode(errs ...error) int {
	if errors.Join(errs...) != nil {
		return 1
	}
	return 0
}

func newLogger(cfg config.ServerLoggingConfig) (*slog.Logger, func() error, error) {
	level := slog.LevelInfo
	switch cfg.Level {
	case "debug":
		level = slog.LevelDebug
	case "warn":
		level = slog.LevelWarn
	case "error":
		level = slog.LevelError
	}
	opts := &slog.HandlerOptions{Level: level}
	if cfg.Output == "" {
		return slog.New(slog.NewJSONHandler(os.Stdout, opts)), func() error { return nil }, nil
	}
	file, err := os.OpenFile(cfg.Output, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return nil, nil, err
	}
	return slog.New(slog.NewJSONHandler(file, opts)), file.Close, nil
}

func run(cfg *config.ServerConfig, logger *slog.Logger, allowEphemeralOverData bool) (err error) {
	logSupportBoundary(cfg, logger)
	lock, err := prepareDataDir(cfg, allowEphemeralOverData)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, lock.Close()) }()

	dbEngine, walWriter, snapshotLSN, err := buildEngine(cfg, logger)
	if err != nil {
		return err
	}
	if closer, ok := dbEngine.(io.Closer); ok {
		defer func() { err = errors.Join(err, closer.Close()) }()
	}
	if walWriter != nil {
		defer func() { err = errors.Join(err, walWriter.Close()) }()
	}

	var options []storage.Option
	if walWriter != nil {
		options = append(options, storage.WithWAL(walWriter))
	}
	// A standby starts read-only: client writes are rejected until PROMOTE, while replication applies the master's log
	// directly to the engine.
	if cfg.Replication.Role == config.RoleStandby {
		options = append(options, storage.WithReadOnly(true))
	}
	store := storage.New(dbEngine, options...)

	repl, err := setupReplication(cfg, logger, store, walWriter)
	if err != nil {
		return err
	}

	var computeOptions []compute.Option
	if repl != nil {
		computeOptions = append(computeOptions,
			compute.WithAdmin(repl.admin),
			compute.WithPromoteEnabled(cfg.Replication.AllowRemotePromote))
	}
	comp := compute.New(parser.New(), store, logger, computeOptions...)
	return serve(cfg, logger, comp, store, walWriter, repl, snapshotLSN)
}

func prepareDataDir(cfg *config.ServerConfig, allowEphemeralOverData bool) (*datadir.Lock, error) {
	dir := cfg.WAL.DataDir
	expected := datadir.KindWAL
	durable := cfg.WAL.Enabled
	if cfg.Engine.Type == engine.TypeTiered {
		dir = cfg.Engine.DataDir
		expected = datadir.KindTiered
		durable = true
	}

	var lock *datadir.Lock
	var err error
	if durable {
		lock, err = datadir.Acquire(dir)
		if err != nil {
			return nil, err
		}
	}
	kind, err := datadir.Detect(dir)
	if err != nil {
		return nil, errors.Join(err, lock.Close())
	}
	if !durable {
		if kind != datadir.KindNone && !allowEphemeralOverData {
			return nil, fmt.Errorf(
				"refusing ephemeral startup over %s database files in %q; use -allow-ephemeral-over-data to proceed",
				kind, dir,
			)
		}
		//nolint:nilnil // Ephemeral mode does not own a directory lock.
		return nil, nil
	}
	if kind != datadir.KindNone && kind != expected {
		return nil, errors.Join(
			fmt.Errorf("configured %s storage but found %s database files in %q", expected, kind, dir),
			lock.Close(),
		)
	}
	return lock, nil
}

// logSupportBoundary warns when the configuration selects features outside the GA support boundary: the tiered engine
// and replication are previews with limited support, and an in-memory engine without a WAL holds data only until
// shutdown. The tiered engine runs without a WAL by design (it keeps its own durable store), so it is not ephemeral.
func logSupportBoundary(cfg *config.ServerConfig, logger *slog.Logger) {
	if cfg.Engine.Type == engine.TypeTiered {
		logger.Warn("Preview feature enabled: tiered engine")
	}
	if cfg.Replication.Role != config.RoleStandalone {
		logger.Warn("Preview feature enabled: replication", "role", cfg.Replication.Role)
	}
	if !cfg.WAL.Enabled && cfg.Engine.Type == engine.TypeInMemory {
		logger.Warn("Ephemeral mode: WAL disabled, data is lost on shutdown")
	}
}

// buildEngine constructs the configured storage engine. The tiered engine keeps its own durable segment store (no WAL);
// the in-memory engine recovers from the WAL/snapshot when persistence is enabled, and so returns a writer and the LSN
// to resume from.
func buildEngine( //nolint:ireturn // returns the configured engine (in-memory or tiered) behind storage.Engine
	cfg *config.ServerConfig,
	logger *slog.Logger,
) (storage.Engine, *wal.Writer, uint64, error) {
	if cfg.Engine.Type == engine.TypeTiered {
		tieredEngine, err := tiered.Open(tieredConfig(cfg.Engine), logger)
		if err != nil {
			return nil, nil, 0, fmt.Errorf("open tiered engine: %w", err)
		}
		return tieredEngine, nil, 0, nil
	}
	return recoverPersistence(cfg, logger)
}

func tieredConfig(cfg config.ServerEngineConfig) tiered.Config {
	const mib = 1 << 20
	return tiered.Config{
		Dir:                 cfg.DataDir,
		MaxMemoryBytes:      cfg.MaxMemoryMB * mib,
		MaxStorageBytes:     cfg.MaxStorageMB * mib,
		SegmentSize:         cfg.SegmentSizeMB * mib,
		Sync:                cfg.Sync,
		CompactionThreshold: cfg.CompactionThreshold,
		CompactionInterval:  cfg.CompactionInterval,
	}
}

func recoverPersistence(
	cfg *config.ServerConfig,
	logger *slog.Logger,
) (*engine.Engine, *wal.Writer, uint64, error) {
	dbEngine := engine.New()
	if !cfg.WAL.Enabled {
		return dbEngine, nil, 0, nil
	}

	var entries []engine.Entry
	snapshotLSN, err := wal.LoadLatestSnapshot(cfg.WAL.DataDir, func(table, key, value string) error {
		entries = append(entries, engine.Entry{Table: table, Key: key, Value: value})
		return nil
	})
	if err != nil {
		return nil, nil, 0, fmt.Errorf("load snapshot: %w", err)
	}
	dbEngine.Load(context.Background(), entries)

	lastLSN, err := wal.NewReader(cfg.WAL.DataDir, logger).Replay(snapshotLSN, func(record wal.Record) error {
		return storage.ApplyReplay(context.Background(), dbEngine, record.Command, record.Args)
	})
	if err != nil {
		return nil, nil, 0, fmt.Errorf("replay WAL: %w", err)
	}
	writer, err := wal.OpenWriter(wal.WriterConfig{
		Dir:         cfg.WAL.DataDir,
		Sync:        cfg.WAL.Sync,
		SegmentSize: cfg.WAL.SegmentSizeMB << 20,
	}, lastLSN)
	if err != nil {
		return nil, nil, 0, fmt.Errorf("open WAL: %w", err)
	}
	logger.Info("Persistence recovered", "snapshot_lsn", snapshotLSN, "last_lsn", lastLSN)
	return dbEngine, writer, snapshotLSN, nil
}

func serve(
	cfg *config.ServerConfig,
	logger *slog.Logger,
	comp *compute.Compute,
	store *storage.Storage,
	walWriter *wal.Writer,
	repl *replicationRuntime,
	recoveredSnapshotLSN uint64,
) error {
	srv, err := network.NewTCPServer(cfg.Network.Address, logger,
		network.WithServerIdleTimeout(cfg.Network.IdleTimeout),
		network.WithServerMaxMessageSize(cfg.Network.MaxMessageSizeKB*1024),
		network.WithServerMaxConnections(cfg.Network.MaxConnections))
	if err != nil {
		return errors.Join(err, stopReplication(repl))
	}

	runtimeCtx, cancelRuntime := context.WithCancel(context.Background())
	defer cancelRuntime()
	serverDone := make(chan error, 1)
	go func() {
		serverDone <- srv.Serve(requestHandler(comp))
	}()
	// Snapshots run for every role: a standby applies replicated records through the storage layer under the same lock a
	// snapshot takes, so its snapshots are consistent, and this keeps a promoted node's WAL bounded.
	snapshotDone := startSnapshotLoop(runtimeCtx, cfg, logger, store, walWriter, recoveredSnapshotLSN)
	replDone := startReplication(runtimeCtx, logger, repl)

	logger.Info("Server started", "address", cfg.Network.Address, "role", roleName(cfg.Replication.Role))
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(sigChan)
	<-sigChan
	logger.Info("Shutting down server...")
	shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), cfg.Network.ShutdownTimeout)
	serveErr := srv.Shutdown(shutdownCtx)
	cancelShutdown()
	serveErr = errors.Join(serveErr, <-serverDone)

	cancelRuntime()
	replErr := stopReplication(repl)
	<-replDone
	<-snapshotDone
	return errors.Join(serveErr, replErr)
}

func requestHandler(comp *compute.Compute) network.RequestHandler {
	return func(ctx context.Context, cmd string, args []string) protocol.Reply {
		result, err := comp.HandleRequest(ctx, cmd, args)
		if err == nil {
			return result
		}
		if errors.Is(err, storage.ErrNotFound) {
			return protocol.NullBulkString()
		}
		if errors.Is(err, storage.ErrReadOnly) {
			return protocol.Error("readonly")
		}
		return protocol.Error(err.Error())
	}
}

func roleName(role string) string {
	if role == config.RoleStandalone {
		return "standalone"
	}
	return role
}

func startSnapshotLoop(
	ctx context.Context,
	cfg *config.ServerConfig,
	logger *slog.Logger,
	store *storage.Storage,
	writer *wal.Writer,
	lastSnapshotLSN uint64,
) <-chan struct{} {
	done := make(chan struct{})
	if writer == nil {
		close(done)
		return done
	}
	go func() {
		defer close(done)
		ticker := time.NewTicker(cfg.WAL.SnapshotInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if writer.LastLSN() == lastSnapshotLSN {
					continue
				}
				writtenLSN, err := createSnapshot(ctx, cfg.WAL.DataDir, store)
				if err != nil {
					logger.Error("Failed to write snapshot", "error", err)
					continue
				}
				lastSnapshotLSN = writtenLSN
				logger.Info("Snapshot written", "lsn", writtenLSN)
			}
		}
	}()
	return done
}

func createSnapshot(ctx context.Context, dir string, store *storage.Storage) (uint64, error) {
	var writtenLSN uint64
	err := store.Snapshot(ctx, func(ctx context.Context, lsn uint64, source storage.SnapshotSource) error {
		writtenLSN = lsn
		return wal.WriteSnapshot(ctx, dir, lsn, source)
	})
	return writtenLSN, err
}
