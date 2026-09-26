package status_test

import (
	"bytes"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/OutOfStack/db/internal/engine"
	"github.com/OutOfStack/db/internal/protocol"
	"github.com/OutOfStack/db/internal/status"
	"github.com/OutOfStack/db/internal/storage"
	"github.com/OutOfStack/db/internal/version"
	"github.com/OutOfStack/db/internal/wal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type fixedSource wal.Status

func (s fixedSource) Status() wal.Status { return wal.Status(s) }

// fields decodes a STATUS reply into a map, checking on the way that it is the frozen field list in order.
func fields(t *testing.T, reply protocol.Reply) map[string]string {
	t.Helper()
	require.Equal(t, protocol.ReplyArray, reply.Kind)
	require.Len(t, reply.Array, 2*len(status.Fields))
	values := make(map[string]string, len(status.Fields))
	for i, field := range status.Fields {
		name, value := reply.Array[2*i], reply.Array[2*i+1]
		require.Equal(t, protocol.ReplyBulkString, name.Kind)
		require.Equal(t, protocol.ReplyBulkString, value.Kind)
		require.Equal(t, field, name.Value, "field %d is out of order", i)
		values[field] = value.Value
	}
	return values
}

func report(t *testing.T, reporter *status.Reporter) map[string]string {
	t.Helper()
	reply, err := reporter.Status(t.Context())
	require.NoError(t, err)
	return fields(t, reply)
}

func TestStatusReportsHealthyDurableNode(t *testing.T) {
	t.Parallel()

	synced := time.Date(2026, 9, 25, 10, 0, 0, 0, time.FixedZone("east", 3*60*60))
	source := fixedSource{
		Ready: true, LastLSN: 42, SyncedLSN: 40, SnapshotLSN: 30, LastSync: synced,
	}
	got := report(t, status.New(engine.TypeInMemory, string(wal.SyncEverySec), source,
		func() string { return "master" }))

	info := version.Get()
	assert.Equal(t, map[string]string{
		"ready":           "true",
		"state":           status.StateOK,
		"error":           "",
		"role":            "master",
		"engine":          engine.TypeInMemory,
		"durability":      "everysec",
		"applied_lsn":     "42",
		"synced_lsn":      "40",
		"snapshot_lsn":    "30",
		"last_sync":       "2026-09-25T07:00:00Z", // always UTC
		"last_snapshot":   "",                     // none written by this process
		"release":         info.Release,
		"protocol":        protocol.Version,
		"wal_format":      strconv.Itoa(wal.WALFormatVersion),
		"snapshot_format": strconv.Itoa(wal.SnapshotFormatVersion),
		"segment_format":  strconv.Itoa(info.SegmentFormat),
	}, got)
}

func TestStatusStates(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		source fixedSource
		ready  string
		state  string
		error  string
	}{
		{name: "ok", source: fixedSource{Ready: true}, ready: "true", state: status.StateOK},
		{
			name:   "maintenance failing",
			source: fixedSource{Ready: true, Degraded: true, MaintenanceError: errors.New("snapshot: disk full")},
			ready:  "true", state: status.StateDegraded, error: "snapshot: disk full",
		},
		{
			name: "terminal",
			source: fixedSource{
				Degraded: true, TerminalError: errors.New("sync WAL: input/output error"),
				MaintenanceError: errors.New("snapshot: disk full"),
			},
			ready: "false", state: status.StateTerminal, error: "sync WAL: input/output error",
		},
		// Ready drops while the node shuts down, without anything having failed.
		{name: "closing", source: fixedSource{}, ready: "false", state: status.StateOK},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := report(t, status.New(engine.TypeInMemory, "always", tc.source, nil))
			assert.Equal(t, tc.ready, got["ready"])
			assert.Equal(t, tc.state, got["state"])
			assert.Equal(t, tc.error, got["error"])
			assert.Equal(t, status.RoleStandalone, got["role"], "a nil role function means standalone")
		})
	}
}

func TestStatusTruncatesTheErrorOnARuneBoundary(t *testing.T) {
	t.Parallel()

	// Two-byte runes with an odd limit force the cut into the middle of one.
	long := strings.Repeat("é", status.MaxErrorBytes)
	got := report(t, status.New(engine.TypeTiered, "always",
		fixedSource{Degraded: true, TerminalError: errors.New(long)}, nil))
	assert.LessOrEqual(t, len(got["error"]), status.MaxErrorBytes)
	assert.GreaterOrEqual(t, len(got["error"]), status.MaxErrorBytes-1)
	assert.True(t, utf8.ValidString(got["error"]))
}

// TestStatusSizeDoesNotGrowWithTheDataset is what makes STATUS safe to poll: an ephemeral node, whose LSNs stay zero,
// replies with exactly the same bytes whether it holds nothing or thousands of keys.
func TestStatusSizeDoesNotGrowWithTheDataset(t *testing.T) {
	t.Parallel()

	encodedStatus := func(keys int) []byte {
		store := storage.New(engine.New())
		for i := range keys {
			_, err := store.Execute(t.Context(), "SET", []string{fmt.Sprintf("table-%d", i%50), strconv.Itoa(i), "value"})
			require.NoError(t, err)
		}
		reply, err := status.New(engine.TypeInMemory, status.DurabilityEphemeral, store, nil).Status(t.Context())
		require.NoError(t, err)
		var buf bytes.Buffer
		require.NoError(t, protocol.WriteReply(&buf, reply))
		return buf.Bytes()
	}

	empty := encodedStatus(0)
	assert.Equal(t, empty, encodedStatus(5000))
	assert.Contains(t, string(empty), status.DurabilityEphemeral)
}
