package compat

import (
	"bytes"
	"flag"
	"os"
	"testing"

	"github.com/OutOfStack/db/internal/datadir"
	"github.com/OutOfStack/db/internal/protocol"
	"github.com/OutOfStack/db/internal/wal"
)

// regenerate writes the golden fixtures from the current build instead of checking them. It exists for the next major
// version, which freezes a new set; running it against v1's files would turn a format regression into a committed
// change, so it is a flag rather than a shipped tool and the reader tests run without it.
//
//	go test ./internal/compat -run TestGenerateGolden -compat.regenerate
var regenerate = flag.Bool("compat.regenerate", false, //nolint:gochecknoglobals // test flags are package-level by nature
	"rewrite the golden fixtures from this build; never run this against an already-frozen version")

func TestGenerateGolden(t *testing.T) {
	if !*regenerate {
		t.Skip("pass -compat.regenerate to rewrite the fixtures; see the flag's documentation first")
	}

	for _, set := range goldenWireSets {
		writeFixture(t, goldenPath(set.requestsFile), encodeRequests(t, set.requests))
		writeFixture(t, goldenPath(set.repliesFile), encodeReplies(t, set.replies))
	}
	generateData(t)
}

// generateData writes the snapshot and the WAL segment through the same writers the server uses, so the fixture is the
// real format rather than a re-implementation of it.
func generateData(t *testing.T) {
	t.Helper()

	dir := goldenPath(dataDir)
	if err := os.RemoveAll(dir); err != nil {
		t.Fatalf("clear fixture data directory: %v", err)
	}
	if err := os.MkdirAll(dir, 0o750); err != nil {
		t.Fatalf("create fixture data directory: %v", err)
	}

	ctx := t.Context()
	if err := wal.WriteSnapshot(ctx, dir, goldenSnapshotLSN, snapshotSource(goldenSnapshotEntries)); err != nil {
		t.Fatalf("write fixture snapshot: %v", err)
	}

	// The writer assigns LSNs itself, continuing from the snapshot's, so the records land at the LSNs goldenRecords
	// declares.
	writer, err := wal.OpenWriter(wal.WriterConfig{Dir: dir, Sync: wal.SyncAlways, SegmentSize: 1 << 20}, goldenSnapshotLSN)
	if err != nil {
		t.Fatalf("open fixture WAL: %v", err)
	}
	for _, record := range goldenRecords {
		lsn, aErr := writer.Append(ctx, record.Command, record.Args)
		if aErr != nil {
			t.Fatalf("append fixture record: %v", aErr)
		}
		if lsn != record.LSN {
			t.Fatalf("fixture record got LSN %d, want %d", lsn, record.LSN)
		}
	}
	if err = writer.Close(); err != nil {
		t.Fatalf("close fixture WAL: %v", err)
	}
	if err = datadir.WriteManifest(dir, goldenManifest); err != nil {
		t.Fatalf("write fixture manifest: %v", err)
	}
}

func writeFixture(t *testing.T, path string, content []byte) {
	t.Helper()
	if err := os.WriteFile(path, content, 0o600); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}

// encodeRequests and encodeReplies produce the byte streams the reader test compares against. Both the generator and
// the reader go through them, so a fixture is defined by exactly one encoding path.
func encodeRequests(t *testing.T, requests []goldenRequest) []byte {
	t.Helper()
	var buf bytes.Buffer
	for _, request := range requests {
		if err := protocol.WriteCommand(&buf, request.cmd, request.args); err != nil {
			t.Fatalf("encode %s: %v", request.cmd, err)
		}
	}
	return buf.Bytes()
}

func encodeReplies(t *testing.T, replies []protocol.Reply) []byte {
	t.Helper()
	var buf bytes.Buffer
	for i, reply := range replies {
		if err := protocol.WriteReply(&buf, reply); err != nil {
			t.Fatalf("encode reply %d: %v", i, err)
		}
	}
	return buf.Bytes()
}

// snapshotSource adapts the fixture entries to the snapshot writer, preserving their order so the file is byte-stable
// across regenerations.
type snapshotSource []struct {
	table, key string
	value      protocol.Value
}

func (s snapshotSource) Range(fn func(table, key, value string) bool) {
	for _, entry := range s {
		if !fn(entry.table, entry.key, protocol.Encode(entry.value)) {
			return
		}
	}
}

// ensure the generator's adapter keeps matching the writer's contract even when the fixture list changes shape.
var _ wal.SnapshotSource = snapshotSource(nil)
