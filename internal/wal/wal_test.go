package wal_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/OutOfStack/db/internal/wal"
)

func TestRecordRoundTripAndChecksum(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	writer, err := wal.OpenWriter(wal.WriterConfig{Dir: dir, Sync: wal.SyncAlways, SegmentSize: 1}, 0)
	if err != nil {
		t.Fatal(err)
	}
	want := wal.Record{LSN: 1, Command: wal.CommandSet, Args: []string{"users", "name", "value\nwith\x00bytes"}}
	if _, err = writer.Append(t.Context(), want.Command, want.Args); err != nil {
		t.Fatal(err)
	}
	if _, err = writer.Append(t.Context(), wal.CommandSet, []string{"users", "second", "value"}); err != nil {
		t.Fatal(err)
	}
	if err = writer.Close(); err != nil {
		t.Fatal(err)
	}

	var got wal.Record
	_, err = wal.NewReader(dir, nil).Replay(0, func(record wal.Record) error {
		if record.LSN == want.LSN {
			got = record
		}
		return nil
	})
	if err != nil || !reflect.DeepEqual(got, want) {
		t.Fatalf("Replay() record = %#v, %v; want %#v, nil", got, err, want)
	}

	segments := walSegmentFiles(t, dir)
	data, err := os.ReadFile(segments[0])
	if err != nil {
		t.Fatal(err)
	}
	data[len(data)-1] ^= 0xff
	if err = os.WriteFile(segments[0], data, 0o600); err != nil {
		t.Fatal(err)
	}
	_, err = wal.NewReader(dir, nil).Replay(0, func(wal.Record) error { return nil })
	if !errors.Is(err, wal.ErrChecksum) {
		t.Fatalf("Replay() error = %v, want ErrChecksum", err)
	}
}

func walSegmentFiles(t *testing.T, dir string) []string {
	t.Helper()
	segments, err := filepath.Glob(filepath.Join(dir, "wal-*.log"))
	if err != nil {
		t.Fatal(err)
	}
	sort.Strings(segments)
	if len(segments) == 0 {
		t.Fatal("no WAL segments found")
	}
	return segments
}

func TestWriterRotatesAndReaderReplays(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	writer, err := wal.OpenWriter(wal.WriterConfig{Dir: dir, Sync: wal.SyncAlways, SegmentSize: 1}, 0)
	if err != nil {
		t.Fatalf("OpenWriter() error = %v", err)
	}
	for index := range 3 {
		lsn, appendErr := writer.Append(t.Context(), wal.CommandSet, []string{"t", string(rune('a' + index)), "v"})
		if appendErr != nil {
			t.Fatalf("Append() error = %v", appendErr)
		}
		if lsn != uint64(index+1) {
			t.Fatalf("Append() LSN = %d, want %d", lsn, index+1)
		}
	}
	if err = writer.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	segments := walSegmentFiles(t, dir)
	if len(segments) != 3 {
		t.Fatalf("segments = %d, want 3", len(segments))
	}

	var records []wal.Record
	lastLSN, err := wal.NewReader(dir, nil).Replay(0, func(record wal.Record) error {
		records = append(records, record)
		return nil
	})
	if err != nil {
		t.Fatalf("Replay() error = %v", err)
	}
	if lastLSN != 3 || len(records) != 3 {
		t.Fatalf("Replay() = LSN %d, %d records; want 3, 3", lastLSN, len(records))
	}
}

func TestReaderTruncatesPartialLastSegment(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	writer, err := wal.OpenWriter(wal.WriterConfig{Dir: dir, Sync: wal.SyncAlways, SegmentSize: 1 << 20}, 0)
	if err != nil {
		t.Fatalf("OpenWriter() error = %v", err)
	}
	for _, key := range []string{"a", "b"} {
		if _, err = writer.Append(t.Context(), wal.CommandSet, []string{"t", key, "v"}); err != nil {
			t.Fatalf("Append() error = %v", err)
		}
	}
	if err = writer.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	segments := walSegmentFiles(t, dir)
	before, err := os.Stat(segments[0])
	if err != nil {
		t.Fatal(err)
	}
	file, err := os.OpenFile(segments[0], os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = file.Write([]byte{0, 0, 0, 0}); err != nil {
		t.Fatal(err)
	}
	if err = file.Close(); err != nil {
		t.Fatal(err)
	}

	var count int
	lastLSN, err := wal.NewReader(dir, nil).Replay(0, func(wal.Record) error { count++; return nil })
	if err != nil {
		t.Fatalf("Replay() error = %v", err)
	}
	after, err := os.Stat(segments[0])
	if err != nil {
		t.Fatal(err)
	}
	if lastLSN != 2 || count != 2 || after.Size() != before.Size() {
		t.Fatalf("recovery = LSN %d, count %d, size %d; want 2, 2, %d", lastLSN, count, after.Size(), before.Size())
	}
}

func TestReaderRejectsCorruptionInEarlierSegment(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	writer, err := wal.OpenWriter(wal.WriterConfig{Dir: dir, Sync: wal.SyncAlways, SegmentSize: 1}, 0)
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"a", "b"} {
		if _, err = writer.Append(t.Context(), wal.CommandSet, []string{"t", key, "v"}); err != nil {
			t.Fatal(err)
		}
	}
	if err = writer.Close(); err != nil {
		t.Fatal(err)
	}
	segments := walSegmentFiles(t, dir)
	data, err := os.ReadFile(segments[0])
	if err != nil {
		t.Fatal(err)
	}
	data[len(data)-1] ^= 0xff
	if err = os.WriteFile(segments[0], data, 0o600); err != nil {
		t.Fatal(err)
	}

	_, err = wal.NewReader(dir, nil).Replay(0, func(wal.Record) error { return nil })
	if !errors.Is(err, wal.ErrChecksum) {
		t.Fatalf("Replay() error = %v, want checksum error", err)
	}
}

func TestSnapshotAndWALTailRecoveryEquivalence(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	want := newTestState()
	writer, err := wal.OpenWriter(wal.WriterConfig{Dir: dir, Sync: wal.SyncAlways, SegmentSize: 128}, 0)
	if err != nil {
		t.Fatal(err)
	}
	apply := func(command string, args []string) {
		if command == wal.CommandSet {
			want.set(args[0], args[1], args[2])
		} else {
			want.del(args[0], args[1])
		}
	}
	appendMutation := func(command string, args []string) {
		t.Helper()
		if _, appendErr := writer.Append(t.Context(), command, args); appendErr != nil {
			t.Fatal(appendErr)
		}
		apply(command, args)
	}

	appendMutation(wal.CommandSet, []string{"users", "a", "one"})
	appendMutation(wal.CommandSet, []string{"users", "b", "two"})
	snapshotLSN := writer.LastLSN()
	if err = wal.WriteSnapshot(t.Context(), dir, snapshotLSN, want); err != nil {
		t.Fatal(err)
	}
	if err = writer.Prune(t.Context(), snapshotLSN); err != nil {
		t.Fatal(err)
	}
	appendMutation(wal.CommandSet, []string{"orders", "x", "three"})
	appendMutation(wal.CommandDel, []string{"users", "a"})
	if err = writer.Close(); err != nil {
		t.Fatal(err)
	}

	got := newTestState()
	loadedLSN, err := wal.LoadLatestSnapshot(dir, func(table, key, value string) error {
		got.set(table, key, value)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	lastLSN, err := wal.NewReader(dir, nil).Replay(loadedLSN, func(record wal.Record) error {
		if record.Command == wal.CommandSet {
			got.set(record.Args[0], record.Args[1], record.Args[2])
		} else {
			got.del(record.Args[0], record.Args[1])
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if lastLSN != 4 || !reflect.DeepEqual(got.values, want.values) {
		t.Fatalf("recovered LSN/state = %d/%v, want 4/%v", lastLSN, got.values, want.values)
	}
}

type testState struct{ values map[string]map[string]string }

func newTestState() *testState { return &testState{values: make(map[string]map[string]string)} }

func (s *testState) set(table, key, value string) {
	if s.values[table] == nil {
		s.values[table] = make(map[string]string)
	}
	s.values[table][key] = value
}

func (s *testState) del(table, key string) {
	delete(s.values[table], key)
	if len(s.values[table]) == 0 {
		delete(s.values, table)
	}
}

func (s *testState) Range(fn func(table, key, value string) bool) {
	for table, values := range s.values {
		for key, value := range values {
			if !fn(table, key, value) {
				return
			}
		}
	}
}

func BenchmarkAppendSyncPolicies(b *testing.B) {
	for _, policy := range []wal.SyncPolicy{wal.SyncAlways, wal.SyncEverySec, wal.SyncNo} {
		b.Run(string(policy), func(b *testing.B) {
			dir := b.TempDir()
			writer, err := wal.OpenWriter(wal.WriterConfig{Dir: dir, Sync: policy, SegmentSize: 64 << 20}, 0)
			if err != nil {
				b.Fatal(err)
			}
			b.ResetTimer()
			for range b.N {
				if _, err = writer.Append(context.Background(), wal.CommandSet, []string{"t", "k", "value"}); err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()
			if err = writer.Close(); err != nil {
				b.Fatal(err)
			}
		})
	}
}

func TestSnapshotIgnoresTemporaryFiles(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "snapshot-999.db.tmp"), []byte("partial"), 0o600); err != nil {
		t.Fatal(err)
	}
	lsn, err := wal.LoadLatestSnapshot(dir, func(string, string, string) error { return nil })
	if err != nil || lsn != 0 {
		t.Fatalf("LoadLatestSnapshot() = %d, %v; want 0, nil", lsn, err)
	}
}

func TestSubscribePublishesCommittedRecords(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	writer, err := wal.OpenWriter(wal.WriterConfig{Dir: dir, Sync: wal.SyncAlways, SegmentSize: 1 << 20}, 0)
	if err != nil {
		t.Fatalf("OpenWriter() error = %v", err)
	}
	defer func() { _ = writer.Close() }()

	sub, unsub := writer.Subscribe()
	defer unsub()

	if _, err = writer.Append(t.Context(), wal.CommandSet, []string{"t", "k", "v"}); err != nil {
		t.Fatalf("Append() error = %v", err)
	}

	select {
	case record := <-sub:
		if record.LSN != 1 || record.Command != wal.CommandSet {
			t.Fatalf("published record = %+v, want LSN 1 SET", record)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for published record")
	}
}

func TestAppendRecordEnforcesContiguityAndReset(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	writer, err := wal.OpenWriter(wal.WriterConfig{Dir: dir, Sync: wal.SyncAlways, SegmentSize: 1 << 20}, 0)
	if err != nil {
		t.Fatalf("OpenWriter() error = %v", err)
	}
	defer func() { _ = writer.Close() }()

	// A standby persists records with the master's LSNs.
	if err = writer.AppendRecord(t.Context(), wal.Record{LSN: 1, Command: wal.CommandSet, Args: []string{"t", "k", "v"}}); err != nil {
		t.Fatalf("AppendRecord(1) error = %v", err)
	}
	// A gap is rejected.
	if err = writer.AppendRecord(t.Context(), wal.Record{LSN: 3, Command: wal.CommandSet, Args: []string{"t", "k", "v"}}); err == nil {
		t.Fatal("AppendRecord(3) after 1 should fail on the LSN gap")
	}
	if writer.LastLSN() != 1 {
		t.Fatalf("LastLSN = %d, want 1", writer.LastLSN())
	}

	// Reset to a snapshot LSN, then continue from there.
	if err = writer.Reset(t.Context(), 10); err != nil {
		t.Fatalf("Reset() error = %v", err)
	}
	if writer.LastLSN() != 10 {
		t.Fatalf("LastLSN after reset = %d, want 10", writer.LastLSN())
	}
	if err = writer.AppendRecord(t.Context(), wal.Record{LSN: 11, Command: wal.CommandSet, Args: []string{"t", "k2", "v2"}}); err != nil {
		t.Fatalf("AppendRecord(11) after reset error = %v", err)
	}
}
