package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/OutOfStack/db/internal/compute"
	"github.com/OutOfStack/db/internal/config"
	"github.com/OutOfStack/db/internal/engine"
	"github.com/OutOfStack/db/internal/network"
	"github.com/OutOfStack/db/internal/parser"
	"github.com/OutOfStack/db/internal/protocol"
	"github.com/OutOfStack/db/internal/storage"
	"github.com/OutOfStack/db/internal/wal"
)

func main() {
	os.Exit(execute())
}

// execute runs the server and returns a process exit code. It is separate from
// main so deferred cleanup (e.g. closing the log file) runs before os.Exit.
func execute() int {
	var configPath string
	flag.StringVar(&configPath, "config", "", "Path to configuration file")
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
	defer func() {
		if closeErr := closeLog(); closeErr != nil {
			log.Printf("Failed to close log file: %v", closeErr)
		}
	}()

	if err = run(cfg, logger); err != nil {
		logger.Error("Server stopped", "error", err)
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

func run(cfg *config.ServerConfig, logger *slog.Logger) error {
	dbEngine, walWriter, snapshotLSN, err := recoverPersistence(cfg, logger)
	if err != nil {
		return err
	}
	if walWriter != nil {
		defer func() { _ = walWriter.Close() }() // the coordinated shutdown below reports the first close error
	}

	var options []storage.Option
	if walWriter != nil {
		options = append(options, storage.WithWAL(walWriter))
	}
	// A standby starts read-only: client writes are rejected until PROMOTE, while
	// replication applies the master's log directly to the engine.
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
		computeOptions = append(computeOptions, compute.WithAdmin(repl.admin))
	}
	comp := compute.New(parser.New(), store, logger, computeOptions...)
	return serve(cfg, logger, comp, store, walWriter, repl, snapshotLSN)
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
		return applyRecoveredRecord(dbEngine, record)
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

func applyRecoveredRecord(dbEngine *engine.Engine, record wal.Record) error {
	switch record.Command {
	case wal.CommandSet:
		return dbEngine.Set(context.Background(), record.Args[0], record.Args[1], record.Args[2])
	case wal.CommandDel:
		err := dbEngine.Del(context.Background(), record.Args[0], record.Args[1])
		if errors.Is(err, engine.ErrNotFound) {
			return nil
		}
		return err
	default:
		return fmt.Errorf("unsupported WAL command %q", record.Command)
	}
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
		return err
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	serverDone := make(chan struct{})
	go func() {
		defer close(serverDone)
		srv.Start(ctx, requestHandler(comp))
	}()
	// Snapshots run for every role: a standby applies replicated records through
	// the storage layer under the same lock a snapshot takes, so its snapshots
	// are consistent, and this keeps a promoted node's WAL bounded.
	snapshotDone := startSnapshotLoop(ctx, cfg, logger, store, walWriter, recoveredSnapshotLSN)
	replDone := startReplication(ctx, logger, repl)

	logger.Info("Server started", "address", cfg.Network.Address, "role", roleName(cfg.Replication.Role))
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(sigChan)
	<-sigChan

	logger.Info("Shutting down server...")
	cancel()
	stopReplication(logger, repl)
	<-replDone
	<-serverDone
	<-snapshotDone
	if walWriter != nil {
		if err = walWriter.Close(); err != nil {
			return fmt.Errorf("close WAL: %w", err)
		}
	}
	return nil
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
