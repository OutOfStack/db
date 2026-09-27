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

	"github.com/OutOfStack/db/internal/datadir"
	"github.com/OutOfStack/db/internal/engine"
	"github.com/OutOfStack/db/internal/protocol"
	"github.com/OutOfStack/db/internal/status"
	"github.com/OutOfStack/db/internal/storage"
	"github.com/OutOfStack/db/internal/wal"
)

// TestGoldenRequestsDecode checks that this build decodes each v1 request stream into the commands it recorded.
func TestGoldenRequestsDecode(t *testing.T) {
	t.Parallel()

	for _, set := range goldenWireSets {
		t.Run(set.name, func(t *testing.T) {
			t.Parallel()
			reader := bufio.NewReader(bytes.NewReader(readFixture(t, set.requestsFile)))
			for i, want := range set.requests {
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
			expectEOF(t, reader, set.requestsFile)
		})
	}
}

// TestGoldenRepliesDecode checks that this build decodes each v1 reply stream, error codes included, into the replies
// it recorded.
func TestGoldenRepliesDecode(t *testing.T) {
	t.Parallel()

	for _, set := range goldenWireSets {
		t.Run(set.name, func(t *testing.T) {
			t.Parallel()
			reader := bufio.NewReader(bytes.NewReader(readFixture(t, set.repliesFile)))
			for i, want := range set.replies {
				got, err := protocol.ReadReply(reader, 0)
				if err != nil {
					t.Fatalf("reply %d: %v", i, err)
				}
				if !reflect.DeepEqual(got, want) {
					t.Errorf("reply %d: decoded %#v, want %#v", i, got, want)
				}
			}
			expectEOF(t, reader, set.repliesFile)
		})
	}
}

// TestGoldenWireBytesAreStable checks the other direction: encoding the recorded commands and replies with this build
// reproduces the fixtures byte for byte. Decoding alone would miss an encoder that started emitting a different — still
// readable — framing.
func TestGoldenWireBytesAreStable(t *testing.T) {
	t.Parallel()

	for _, set := range goldenWireSets {
		for _, tc := range []struct {
			file   string
			encode func(*testing.T) []byte
		}{
			{file: set.requestsFile, encode: func(t *testing.T) []byte {
				t.Helper()
				return encodeRequests(t, set.requests)
			}},
			{file: set.repliesFile, encode: func(t *testing.T) []byte {
				t.Helper()
				return encodeReplies(t, set.replies)
			}},
		} {
			t.Run(tc.file, func(t *testing.T) {
				t.Parallel()
				want := readFixture(t, tc.file)
				if got := tc.encode(t); !bytes.Equal(got, want) {
					t.Errorf("%s encoding drifted from the v1 fixture\n got %q\nwant %q", tc.file, got, want)
				}
			})
		}
	}
}

// TestGoldenStatusFields holds the live STATUS reply to the field names, and their order, the fixture recorded. The
// values are this node's own; the names are the contract a monitoring script parses.
func TestGoldenStatusFields(t *testing.T) {
	t.Parallel()

	recorded := goldenOperationsReplies[1]
	live, err := status.New("in_memory", "everysec", staticStatus{Ready: true}, nil).Status(t.Context())
	if err != nil {
		t.Fatalf("Status: %v", err)
	}
	if len(live.Array) != len(recorded.Array) {
		t.Fatalf("STATUS has %d elements, the v1 fixture %d", len(live.Array), len(recorded.Array))
	}
	for i := 0; i < len(recorded.Array); i += 2 {
		if live.Array[i].Value != recorded.Array[i].Value {
			t.Errorf("STATUS field %d = %q, the v1 fixture has %q", i/2, live.Array[i].Value, recorded.Array[i].Value)
		}
	}
}

type staticStatus wal.Status

func (s staticStatus) Status() wal.Status { return wal.Status(s) }

// TestGoldenManifest checks the v1 MANIFEST: this build encodes the recorded manifest to the same bytes, reads the file
// back to the same value, and accepts the directory it describes.
func TestGoldenManifest(t *testing.T) {
	t.Parallel()

	dir := goldenPath(dataDir)
	encoded, err := datadir.EncodeManifest(goldenManifest)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	if want := readFixture(t, dataDir+"/"+datadir.ManifestName); !bytes.Equal(encoded, want) {
		t.Errorf("manifest encoding drifted from the v1 fixture\n got %q\nwant %q", encoded, want)
	}
	manifest, found, err := datadir.ReadManifest(dir)
	if err != nil || !found {
		t.Fatalf("ReadManifest = found %v, %v", found, err)
	}
	if !reflect.DeepEqual(manifest, goldenManifest) {
		t.Errorf("read %#v, want %#v", manifest, goldenManifest)
	}
	if err = datadir.CheckManifest(dir, manifest, goldenManifest.Engine); err != nil {
		t.Errorf("this build refuses the v1 data directory: %v", err)
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
