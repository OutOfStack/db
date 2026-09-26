package compat

import (
	"bufio"
	"bytes"
	"errors"
	"io"
	"log/slog"
	"os"
	"reflect"
	"sort"
	"testing"

	"github.com/OutOfStack/db/internal/engine"
	"github.com/OutOfStack/db/internal/protocol"
	"github.com/OutOfStack/db/internal/storage"
	"github.com/OutOfStack/db/internal/wal"
)

// TestGoldenRequestsDecode checks that this build decodes the v1 request stream into the commands it recorded.
func TestGoldenRequestsDecode(t *testing.T) {
	t.Parallel()

	reader := bufio.NewReader(bytes.NewReader(readFixture(t, requestsFile)))
	for i, want := range goldenRequests {
		cmd, args, err := protocol.ReadCommand(reader, 0)
		if err != nil {
			t.Fatalf("request %d (%s): %v", i, want.cmd, err)
		}
		if cmd != want.cmd {
			t.Errorf("request %d: command = %q, want %q", i, cmd, want.cmd)
		}
		// An empty argument list decodes to an empty slice rather than nil; compare by content.
		if len(args) != len(want.args) || (len(args) > 0 && !reflect.DeepEqual(args, want.args)) {
			t.Errorf("request %d (%s): args = %q, want %q", i, want.cmd, args, want.args)
		}
	}
	expectEOF(t, reader, "requests")
}

// TestGoldenRepliesDecode checks that this build decodes the v1 reply stream, error codes included, into the replies it
// recorded.
func TestGoldenRepliesDecode(t *testing.T) {
	t.Parallel()

	reader := bufio.NewReader(bytes.NewReader(readFixture(t, repliesFile)))
	for i, want := range goldenReplies {
		got, err := protocol.ReadReply(reader, 0)
		if err != nil {
			t.Fatalf("reply %d: %v", i, err)
		}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("reply %d: decoded %#v, want %#v", i, got, want)
		}
	}
	expectEOF(t, reader, "replies")
}

// TestGoldenWireBytesAreStable checks the other direction: encoding the recorded commands and replies with this build
// reproduces the fixture byte for byte. Decoding alone would miss an encoder that started emitting a different — still
// readable — framing.
func TestGoldenWireBytesAreStable(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name   string
		file   string
		encode func(*testing.T) []byte
	}{
		{name: "requests", file: requestsFile, encode: encodeGoldenRequests},
		{name: "replies", file: repliesFile, encode: encodeGoldenReplies},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			want := readFixture(t, tc.file)
			if got := tc.encode(t); !bytes.Equal(got, want) {
				t.Errorf("%s encoding drifted from the v1 fixture\n got %q\nwant %q", tc.name, got, want)
			}
		})
	}
}

// TestGoldenRecoveryReproducesState replays the v1 snapshot and WAL segment through the real recovery path and checks
// the state they rebuild. This is the fixture that matters most: it fails if a future build would read a v1 data
// directory as anything other than what v1 wrote.
func TestGoldenRecoveryReproducesState(t *testing.T) {
	t.Parallel()

	dir := goldenPath(dataDir)
	ctx := t.Context()

	dbEngine := engine.New()
	var entries []engine.Entry
	snapshotLSN, err := wal.LoadLatestSnapshot(dir, func(table, key, value string) error {
		entries = append(entries, engine.Entry{Table: table, Key: key, Value: value})
		return nil
	})
	if err != nil {
		t.Fatalf("load v1 snapshot: %v", err)
	}
	if snapshotLSN != goldenSnapshotLSN {
		t.Fatalf("snapshot LSN = %d, want %d", snapshotLSN, goldenSnapshotLSN)
	}
	dbEngine.Load(ctx, entries)

	var replayed []wal.Record
	lastLSN, err := wal.NewReader(dir, slog.New(slog.DiscardHandler)).Replay(snapshotLSN, func(record wal.Record) error {
		replayed = append(replayed, record)
		return storage.ApplyReplay(ctx, dbEngine, record.Command, record.Args)
	})
	if err != nil {
		t.Fatalf("replay v1 WAL: %v", err)
	}
	if want := goldenRecords[len(goldenRecords)-1].LSN; lastLSN != want {
		t.Errorf("last LSN = %d, want %d", lastLSN, want)
	}
	if !reflect.DeepEqual(replayed, goldenRecords) {
		t.Errorf("replayed records = %#v, want %#v", replayed, goldenRecords)
	}

	assertState(t, dbEngine)
}

// assertState compares the recovered engine with the frozen state, checking both the stored encoding and the literal it
// renders to.
func assertState(t *testing.T, dbEngine *engine.Engine) {
	t.Helper()

	type stored struct{ table, key, value string }
	var got []stored
	dbEngine.Range(func(table, key, value string) bool {
		got = append(got, stored{table: table, key: key, value: value})
		return true
	})
	sort.Slice(got, func(i, j int) bool {
		if got[i].table != got[j].table {
			return got[i].table < got[j].table
		}
		return got[i].key < got[j].key
	})

	if len(got) != len(goldenState) {
		t.Fatalf("recovered %d keys, want %d: %+v", len(got), len(goldenState), got)
	}
	for i, want := range goldenState {
		if got[i].table != want.table || got[i].key != want.key {
			t.Fatalf("key %d = %s/%s, want %s/%s", i, got[i].table, got[i].key, want.table, want.key)
		}
		if encoded := protocol.Encode(want.value); got[i].value != encoded {
			t.Errorf("%s/%s stored %q, but this build encodes that value as %q", want.table, want.key, got[i].value, encoded)
		}
		if rendered := protocol.Render(protocol.Decode(got[i].value)); rendered != want.rendered {
			t.Errorf("%s/%s renders as %q, want %q", want.table, want.key, rendered, want.rendered)
		}
	}
}

func readFixture(t *testing.T, name string) []byte {
	t.Helper()
	content, err := os.ReadFile(goldenPath(name))
	if err != nil {
		t.Fatalf("read fixture: %v", err)
	}
	return content
}

func expectEOF(t *testing.T, reader *bufio.Reader, name string) {
	t.Helper()
	if _, err := reader.ReadByte(); !errors.Is(err, io.EOF) {
		t.Errorf("%s fixture has trailing bytes the expectations do not cover", name)
	}
}
