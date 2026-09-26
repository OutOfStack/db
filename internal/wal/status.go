package wal

import (
	"fmt"

	"github.com/OutOfStack/db/internal/protocol"
)

// ErrTerminal wraps the first append or fsync failure, after which the writer can no longer promise that acknowledged
// records are durable. Every later append is refused with it until the process restarts. It reports CodeUnavailable,
// like the tiered engine's latch, so a client can tell a node that is refusing all writes from a failure of its own
// command; errors.Is still reaches the underlying I/O error.
var ErrTerminal = protocol.NewError(protocol.CodeUnavailable, "WAL is in a terminal state after an unrecoverable failure")

// Status is a concurrent view of WAL health. SyncedLSN is a lower bound on the durable prefix, initialized to the
// recovered LSN at open and advanced only by successful syncs or a durable snapshot. LastLSN may be ahead of it.
type Status struct {
	Ready            bool
	Degraded         bool
	LastLSN          uint64
	SyncedLSN        uint64
	TerminalError    error
	MaintenanceError error
}

// Status exposes the terminal append/fsync latch separately from retryable snapshot/prune failures.
func (w *Writer) Status() Status {
	w.statusMu.RLock()
	defer w.statusMu.RUnlock()
	return Status{
		Ready:            !w.closing.Load() && w.terminalErr == nil,
		Degraded:         w.terminalErr != nil || w.maintenanceErr != nil,
		LastLSN:          w.lastLSN.Load(),
		SyncedLSN:        w.syncedLSN.Load(),
		TerminalError:    w.terminalErr,
		MaintenanceError: w.maintenanceErr,
	}
}

func (w *Writer) fail(state *writerState, err error) {
	if state.terminalErr == nil {
		state.terminalErr = fmt.Errorf("%w: %w", ErrTerminal, err)
	}
	w.statusMu.Lock()
	w.terminalErr = state.terminalErr
	w.statusMu.Unlock()
}
