package wal

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
		state.terminalErr = err
	}
	w.statusMu.Lock()
	w.terminalErr = state.terminalErr
	w.statusMu.Unlock()
}
