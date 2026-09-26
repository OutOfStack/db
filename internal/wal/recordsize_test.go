package wal_test

import (
	"strings"
	"testing"

	"github.com/OutOfStack/db/internal/protocol"
	"github.com/OutOfStack/db/internal/wal"
)

// TestAppendRejectsOversizedRecordAsTooLarge covers the one limit the WAL enforces itself. The recovery reader will not
// decode a record past maxRecordSize, so persisting one would produce a log this build cannot replay; the rejection is
// a format limit and has to reach the client as TOOLARGE, not as an unclassified error.
//
// It is reachable in a supported configuration: network.max_message_size is configurable with no ceiling below 64 MiB,
// so a server set above that accepts a command whose WAL record the WAL then refuses.
// Not parallel: the two subtests share one writer and assert its LSN, so they have to run in order.
func TestAppendRejectsOversizedRecordAsTooLarge(t *testing.T) {
	writer, err := wal.OpenWriter(wal.WriterConfig{
		Dir:         t.TempDir(),
		Sync:        wal.SyncNo,
		SegmentSize: 1 << 20,
	}, 0)
	if err != nil {
		t.Fatalf("OpenWriter: %v", err)
	}
	defer writer.Close()

	// One byte past the limit once framing is counted, so the check trips on size rather than on anything else.
	oversized := strings.Repeat("a", wal.MaxRecordSize)
	args := []string{"users", "big", oversized}

	t.Run("Append", func(t *testing.T) {
		lsn, aErr := writer.Append(t.Context(), wal.CommandSet, args)
		requireTooLarge(t, aErr)
		if lsn != 0 {
			t.Errorf("Append assigned LSN %d to a rejected record, want 0", lsn)
		}
		if writer.LastLSN() != 0 {
			t.Errorf("LastLSN = %d after a rejected record, want 0", writer.LastLSN())
		}
	})

	t.Run("AppendRecord", func(t *testing.T) {
		record := wal.Record{LSN: 1, Command: wal.CommandSet, Args: args}
		requireTooLarge(t, writer.AppendRecord(t.Context(), record))
	})
}

func requireTooLarge(t *testing.T, err error) {
	t.Helper()
	if err == nil {
		t.Fatal("oversized record was accepted, want a rejection")
	}
	if code := protocol.CodeOf(err); code != protocol.CodeTooLarge {
		t.Errorf("error code = %q, want %q (error: %v)", code, protocol.CodeTooLarge, err)
	}
}
