// Package status builds the reply to STATUS: one node's readiness, durability and identity in a single bounded reply.
//
// The reply is a flat array of alternating field names and values, all bulk strings, in the order of Fields. It never
// carries per-key data or configuration secrets, and its size does not grow with the dataset, so it is safe to poll as
// a readiness probe and safe to serve while the storage is degraded or fenced. The field names, their order, and the
// meaning of each value are part of the v1 wire contract (COMPATIBILITY.md); a later v1.x release may append fields but
// never removes, renames or reorders one.
package status

import (
	"context"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/OutOfStack/db/internal/protocol"
	"github.com/OutOfStack/db/internal/version"
	"github.com/OutOfStack/db/internal/wal"
)

// Values of the durability field besides the sync policies themselves.
const (
	// DurabilityEphemeral is the in-memory engine without a WAL: nothing survives a restart.
	DurabilityEphemeral = "ephemeral"
)

// Values of the state field.
const (
	StateOK       = "ok"
	StateDegraded = "degraded"
	StateTerminal = "terminal"
)

// RoleStandalone is the role of a server that does not replicate.
const RoleStandalone = "standalone"

// MaxErrorBytes caps the error field so a long failure message cannot make the reply unbounded.
const MaxErrorBytes = 256

// Fields is the frozen field order of the STATUS reply.
var Fields = []string{ //nolint:gochecknoglobals // the frozen field list is package-level data by nature
	"ready",         // "true" while the node serves and persists commands
	"state",         // ok, degraded (maintenance failing, still serving), or terminal (restart required)
	"error",         // the terminal or maintenance cause, truncated to MaxErrorBytes; empty when state is ok
	"role",          // standalone, master or standby
	"engine",        // in_memory or tiered
	"durability",    // the sync policy (always, everysec, no), or ephemeral
	"applied_lsn",   // the last LSN written to this node's WAL; 0 without one
	"synced_lsn",    // a lower bound on the LSN known to be fsynced
	"snapshot_lsn",  // the LSN of the newest snapshot on disk; 0 when none
	"last_sync",     // RFC 3339 UTC time of the last successful fsync in this process; empty before the first
	"last_snapshot", // RFC 3339 UTC time of the last snapshot written by this process; empty before the first
	"release",       // the build's release version
	"protocol",      // the wire protocol version
	"wal_format",    // the WAL, snapshot and tiered segment format versions this build reads and writes
	"snapshot_format",
	"segment_format",
}

// Source is the storage view STATUS reads. *storage.Storage implements it.
type Source interface {
	Status() wal.Status
}

// Reporter answers STATUS for one server.
type Reporter struct {
	engine     string
	durability string
	source     Source
	role       func() string
}

// New returns a reporter for a server running engine with the given durability (a sync policy, or
// DurabilityEphemeral). role reports the current replication role and may be nil for a standalone server; it is a
// function because a promoted standby changes role while running.
func New(engine, durability string, source Source, role func() string) *Reporter {
	if role == nil {
		role = func() string { return RoleStandalone }
	}
	return &Reporter{engine: engine, durability: durability, source: source, role: role}
}

// Status builds the reply. It never fails: a node that cannot serve reports that in the reply rather than refusing it.
func (r *Reporter) Status(context.Context) (protocol.Reply, error) {
	health := r.source.Status()
	info := version.Get()

	state, cause := StateOK, ""
	switch {
	case health.TerminalError != nil:
		state, cause = StateTerminal, health.TerminalError.Error()
	case health.Degraded:
		state = StateDegraded
		if health.MaintenanceError != nil {
			cause = health.MaintenanceError.Error()
		}
	}

	values := []string{
		strconv.FormatBool(health.Ready),
		state,
		truncate(cause, MaxErrorBytes),
		r.role(),
		r.engine,
		r.durability,
		strconv.FormatUint(health.LastLSN, 10),
		strconv.FormatUint(health.SyncedLSN, 10),
		strconv.FormatUint(health.SnapshotLSN, 10),
		timestamp(health.LastSync),
		timestamp(health.LastSnapshot),
		info.Release,
		info.Protocol,
		strconv.Itoa(info.WALFormat),
		strconv.Itoa(info.SnapshotFormat),
		strconv.Itoa(info.SegmentFormat),
	}
	pairs := make([]string, 0, 2*len(Fields))
	for i, field := range Fields {
		pairs = append(pairs, field, values[i])
	}
	return protocol.BulkStringArray(pairs), nil
}

func timestamp(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.UTC().Format(time.RFC3339)
}

// truncate cuts s to at most limit bytes without splitting a UTF-8 sequence.
func truncate(s string, limit int) string {
	if len(s) <= limit {
		return s
	}
	cut := limit
	for cut > 0 && !utf8.RuneStart(s[cut]) {
		cut--
	}
	return strings.ToValidUTF8(s[:cut], "")
}
