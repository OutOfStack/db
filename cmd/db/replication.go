package main

import (
	"context"
	"errors"
	"log/slog"
	"strconv"
	"sync"

	"github.com/OutOfStack/db/internal/config"
	"github.com/OutOfStack/db/internal/protocol"
	"github.com/OutOfStack/db/internal/replication"
	"github.com/OutOfStack/db/internal/storage"
	"github.com/OutOfStack/db/internal/wal"
)

// replicationRuntime bundles the replication components for a server, exactly
// one of master/standby is non-nil (both nil for a standalone server).
type replicationRuntime struct {
	master  *replication.Master
	standby *replication.Standby
	admin   *replicationAdmin
}

// setupReplication builds the replication runtime for the configured role. It
// returns nil for a standalone server. Replication requires the WAL, which the
// config validation guarantees is enabled for master/standby roles.
func setupReplication(
	cfg *config.ServerConfig,
	logger *slog.Logger,
	store *storage.Storage,
	writer *wal.Writer,
) (*replicationRuntime, error) {
	switch cfg.Replication.Role {
	case config.RoleStandalone:
		return nil, nil //nolint:nilnil // standalone has no replication runtime
	case config.RoleMaster:
		master, err := replication.NewMaster(cfg.Replication.ListenAddress, writer, cfg.WAL.DataDir, logger)
		if err != nil {
			return nil, err
		}
		logger.Info("Replication master listening", "address", master.Addr().String())
		return &replicationRuntime{
			master: master,
			admin:  &replicationAdmin{store: store, writer: writer, logger: logger, role: config.RoleMaster},
		}, nil
	case config.RoleStandby:
		standby := replication.NewStandby(
			cfg.Replication.MasterAddress, store, cfg.WAL.DataDir,
			writer.LastLSN(), cfg.Replication.ReconnectBackoff, logger)
		return &replicationRuntime{
			standby: standby,
			admin: &replicationAdmin{
				store:      store,
				writer:     writer,
				standby:    standby,
				logger:     logger,
				dir:        cfg.WAL.DataDir,
				listenAddr: cfg.Replication.ListenAddress,
				role:       config.RoleStandby,
			},
		}, nil
	default:
		return nil, errors.New("unsupported replication role: " + cfg.Replication.Role)
	}
}

// startReplication launches replication background work. The returned channel
// closes when a master stops accepting connections; for a standby the loop is
// owned by the standby and drained by stopReplication.
func startReplication(ctx context.Context, _ *slog.Logger, repl *replicationRuntime) <-chan struct{} {
	done := make(chan struct{})
	if repl == nil {
		close(done)
		return done
	}
	switch {
	case repl.master != nil:
		go func() {
			defer close(done)
			repl.master.Serve(ctx)
		}()
	case repl.standby != nil:
		repl.standby.Start(ctx)
		close(done)
	default:
		close(done)
	}
	return done
}

// stopReplication tears down replication during shutdown.
func stopReplication(logger *slog.Logger, repl *replicationRuntime) {
	if repl == nil {
		return
	}
	if repl.master != nil {
		if err := repl.master.Close(); err != nil {
			logger.Error("Failed to close replication master", "error", err)
		}
	}
	if repl.standby != nil {
		repl.standby.Stop()
	}
	if repl.admin != nil {
		repl.admin.close() // stops a master started by promotion, if any
	}
}

// replicationAdmin implements compute.Admin, handling PROMOTE and REPLICATION
// STATUS. Its role changes from standby to master on promotion.
type replicationAdmin struct {
	store      *storage.Storage
	writer     *wal.Writer
	standby    *replication.Standby // nil when started as master
	logger     *slog.Logger
	dir        string
	listenAddr string // when set, promotion serves replication here

	mu             sync.Mutex
	role           string
	promoted       *replication.Master
	promotedCancel context.CancelFunc
}

// Promote flips a standby to master: it stops replication, lifts read-only mode
// so the server accepts writes, and — when a listen address is configured —
// starts serving replication so other standbys can be re-aimed here. Demoting
// the old master remains an operator action.
func (a *replicationAdmin) Promote(_ context.Context) (protocol.Reply, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.role == config.RoleMaster {
		return protocol.Reply{}, errors.New("server is already master")
	}
	if a.standby != nil {
		a.standby.Stop()
	}
	a.store.Promote()
	a.role = config.RoleMaster
	a.logger.Info("Promoted standby to master", "applied_lsn", a.writer.LastLSN())

	if a.listenAddr != "" {
		// The promoted master outlives this request, so it runs on its own
		// lifetime context rather than the request context.
		if err := a.startMasterLocked(); err != nil { //nolint:contextcheck // intentional: master uses its own lifetime context
			// Writes are already enabled; log rather than fail the promotion.
			a.logger.Error("Promoted master could not serve replication", "error", err)
		}
	}
	return protocol.SimpleString("OK"), nil
}

// startMasterLocked starts a replication listener for the promoted node. The
// caller holds a.mu.
func (a *replicationAdmin) startMasterLocked() error {
	master, err := replication.NewMaster(a.listenAddr, a.writer, a.dir, a.logger)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithCancel(context.Background())
	a.promoted = master
	a.promotedCancel = cancel
	go master.Serve(ctx)
	a.logger.Info("Promoted master serving replication", "address", master.Addr().String())
	return nil
}

// close stops a replication listener started by promotion. It is called during
// server shutdown.
func (a *replicationAdmin) close() {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.promotedCancel != nil {
		a.promotedCancel()
	}
	if a.promoted != nil {
		if err := a.promoted.Close(); err != nil {
			a.logger.Error("Failed to close promoted replication master", "error", err)
		}
	}
}

// Status returns role, applied LSN, lag, and connection state as a flat
// key/value array reply.
func (a *replicationAdmin) Status(_ context.Context) (protocol.Reply, error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	var applied, lag uint64
	connected := true
	if a.role == config.RoleStandby && a.standby != nil {
		applied = a.standby.AppliedLSN()
		lag = a.standby.Lag()
		connected = a.standby.Connected()
	} else {
		applied = a.writer.LastLSN()
	}

	values := []string{
		"role", roleName(a.role),
		"applied_lsn", strconv.FormatUint(applied, 10),
		"lag", strconv.FormatUint(lag, 10),
		"connected", strconv.FormatBool(connected),
	}
	return protocol.BulkStringArray(values), nil
}
