package wal_test

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/OutOfStack/db/internal/engine"
	"github.com/OutOfStack/db/internal/storage"
	"github.com/OutOfStack/db/internal/wal"
	"github.com/stretchr/testify/require"
)

func TestRecoveryChecksumDamageNeverTruncates(t *testing.T) {
	t.Parallel()
	for _, damaged := range []int{1, 2} {
		t.Run(fmt.Sprintf("record-%d", damaged+1), func(t *testing.T) {
			t.Parallel()
			dir := t.TempDir()
			writer, err := wal.OpenWriter(wal.WriterConfig{Dir: dir, Sync: wal.SyncAlways, SegmentSize: 1 << 20}, 0)
			require.NoError(t, err)
			records := make([][]byte, 0, 3)
			for i := range 3 {
				args := []string{"t", strconv.Itoa(i), "value"}
				lsn, appendErr := writer.Append(t.Context(), wal.CommandSet, args)
				require.NoError(t, appendErr)
				record, encodeErr := wal.EncodeRecord(wal.Record{LSN: lsn, Command: wal.CommandSet, Args: args})
				require.NoError(t, encodeErr)
				records = append(records, record)
			}
			require.NoError(t, writer.Close())
			path := walSegmentFiles(t, dir)[0]
			data, err := os.ReadFile(path)
			require.NoError(t, err)
			offset := len(data)
			for _, record := range records {
				offset -= len(record)
			}
			for _, record := range records[:damaged] {
				offset += len(record)
			}
			// Flip payload rather than framing so the decoder reaches the checksum in both middle and final records.
			valueOffset := bytes.Index(records[damaged], []byte("value"))
			require.GreaterOrEqual(t, valueOffset, 0)
			data[offset+valueOffset] ^= 1
			require.NoError(t, os.WriteFile(path, data, 0o600))
			_, err = wal.NewReader(dir, nil).Replay(0, func(wal.Record) error { return nil })
			require.ErrorIs(t, err, wal.ErrChecksum)
			require.ErrorContains(t, err, path)
			require.ErrorContains(t, err, fmt.Sprintf("offset %d", offset))
			after, err := os.ReadFile(path)
			require.NoError(t, err)
			require.True(t, bytes.Equal(data, after))
			require.ErrorIs(t, wal.ReadRecordsFrom(dir, 1, func(wal.Record) error { return nil }), wal.ErrChecksum)
		})
	}
}

func TestRecoveryPartialTailAtEveryByte(t *testing.T) {
	t.Parallel()
	first, err := wal.EncodeRecord(wal.Record{LSN: 1, Command: wal.CommandSet, Args: []string{"t", "first", "value"}})
	require.NoError(t, err)
	tail, err := wal.EncodeRecord(wal.Record{LSN: 2, Command: wal.CommandSet, Args: []string{"t", "second", "value"}})
	require.NoError(t, err)
	for cut := 1; cut < len(tail); cut++ {
		t.Run(strconv.Itoa(cut), func(t *testing.T) {
			t.Parallel()
			dir := t.TempDir()
			path := filepath.Join(dir, "wal-00000000000000000001.log")
			prefix := append([]byte("DBWAL\x00\x02"), first...)
			data := append(bytes.Clone(prefix), tail[:cut]...)
			require.NoError(t, os.WriteFile(path, data, 0o600))
			var got []wal.Record
			lsn, replayErr := wal.NewReader(dir, nil).Replay(0, func(record wal.Record) error { got = append(got, record); return nil })
			require.NoError(t, replayErr)
			require.Equal(t, uint64(1), lsn)
			require.Len(t, got, 1)
			after, readErr := os.ReadFile(path)
			require.NoError(t, readErr)
			require.Equal(t, prefix, after)
		})
	}
}

func TestSnapshotDamageRetainsWALAndReplays(t *testing.T) {
	t.Parallel()
	cases := map[string]func([]byte) []byte{
		"body":           func(data []byte) []byte { data[bytes.Index(data, []byte("value"))] ^= 1; return data },
		"checksum":       func(data []byte) []byte { data[len(data)-12] ^= 1; return data },
		"marker":         func(data []byte) []byte { data[len(data)-1] ^= 1; return data },
		"no trailer":     func(data []byte) []byte { return data[:len(data)-12] },
		"short trailer":  func(data []byte) []byte { return data[:len(data)-1] },
		"body truncated": func(data []byte) []byte { return data[:len(data)/2] },
		"empty":          func([]byte) []byte { return nil },
	}
	for name, damage := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			dir := t.TempDir()
			writer, err := wal.OpenWriter(wal.WriterConfig{Dir: dir, Sync: wal.SyncAlways, SegmentSize: 1}, 0)
			require.NoError(t, err)
			defer func() { require.NoError(t, writer.Close()) }()
			state := newTestState()
			for i := range 3 {
				key := strconv.Itoa(i)
				_, err = writer.Append(t.Context(), wal.CommandSet, []string{"t", key, "value"})
				require.NoError(t, err)
				state.set("t", key, "value")
			}
			require.NoError(t, wal.WriteSnapshot(t.Context(), dir, 3, state))
			_, path, _, err := wal.LatestSnapshotInfo(dir)
			require.NoError(t, err)
			data, err := os.ReadFile(path)
			require.NoError(t, err)
			data = damage(data)
			require.NoError(t, os.WriteFile(path, data, 0o600))
			segments := walSegmentFiles(t, dir)
			require.ErrorIs(t, writer.Prune(t.Context(), 3), wal.ErrInvalidSnapshot)
			require.Equal(t, segments, walSegmentFiles(t, dir))
			require.True(t, writer.Status().Degraded)
			require.True(t, writer.Status().Ready)
			called := false
			apply := func(_, _, _ string) error { called = true; return nil }
			require.ErrorIs(t, wal.ReadSnapshot(bufio.NewReader(bytes.NewReader(data)), apply), wal.ErrInvalidSnapshot)
			lsn, err := wal.LoadLatestSnapshot(dir, apply)
			require.NoError(t, err)
			require.Zero(t, lsn)
			require.False(t, called)
			recovered := newTestState()
			lsn, err = wal.NewReader(dir, nil).Replay(lsn, func(record wal.Record) error {
				recovered.set(record.Args[0], record.Args[1], record.Args[2])
				return nil
			})
			require.NoError(t, err)
			require.Equal(t, uint64(3), lsn)
			require.Equal(t, state, recovered)
			after, err := os.ReadFile(path)
			require.NoError(t, err)
			require.True(t, bytes.Equal(data, after))
			_, err = writer.Append(t.Context(), wal.CommandSet, []string{"t", "new", "value"})
			require.NoError(t, err)
		})
	}
}

func TestRejectedSnapshotWithoutWALFailsClosed(t *testing.T) {
	t.Parallel()
	for _, retained := range []int{0, 1} {
		t.Run(strconv.Itoa(retained), func(t *testing.T) {
			t.Parallel()
			dir := t.TempDir()
			state := newTestState()
			state.set("t", "key", "value")
			require.NoError(t, wal.WriteSnapshot(t.Context(), dir, 2, state))
			_, path, _, err := wal.LatestSnapshotInfo(dir)
			require.NoError(t, err)
			data, err := os.ReadFile(path)
			require.NoError(t, err)
			require.NoError(t, os.WriteFile(path, data[:len(data)-1], 0o600))
			if retained != 0 {
				writer, openErr := wal.OpenWriter(wal.WriterConfig{Dir: dir, Sync: wal.SyncAlways, SegmentSize: 1}, 0)
				require.NoError(t, openErr)
				_, err = writer.Append(t.Context(), wal.CommandSet, []string{"t", "key", "old"})
				require.NoError(t, err)
				require.NoError(t, writer.Close())
			}
			_, err = wal.LoadLatestSnapshot(dir, func(_, _, _ string) error { t.Fatal("applied invalid snapshot"); return nil })
			require.ErrorIs(t, err, wal.ErrInvalidSnapshot)
			require.ErrorContains(t, err, "cannot replace rejected snapshot")
		})
	}
}

func TestSnapshotFsyncFailureDegradesButAppendsContinue(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	writer, err := wal.OpenWriter(wal.WriterConfig{Dir: dir, Sync: wal.SyncAlways, SegmentSize: 1}, 0)
	require.NoError(t, err)
	defer func() { require.NoError(t, writer.Close()) }()
	store := storage.New(engine.New(), storage.WithWAL(writer))
	_, err = store.Execute(t.Context(), "SET", []string{"t", "key", "value"})
	require.NoError(t, err)
	segments := walSegmentFiles(t, dir)
	failure := errors.New("injected snapshot fsync failure")
	err = store.Snapshot(t.Context(), func(ctx context.Context, lsn uint64, source storage.SnapshotSource) error {
		return wal.WriteSnapshotWithSync(ctx, dir, lsn, source, func(*os.File) error { return failure })
	})
	require.ErrorIs(t, err, failure)
	status := store.Status()
	require.True(t, status.Ready)
	require.True(t, status.Degraded)
	require.NoError(t, status.TerminalError)
	require.ErrorIs(t, status.MaintenanceError, failure)
	require.Equal(t, segments, walSegmentFiles(t, dir))
	_, _, ok, err := wal.LatestSnapshotInfo(dir)
	require.NoError(t, err)
	require.False(t, ok)
	_, err = store.Execute(t.Context(), "SET", []string{"t", "next", "value"})
	require.NoError(t, err)
	require.Equal(t, uint64(2), writer.LastLSN())
	require.NoError(t, store.Snapshot(t.Context(), func(ctx context.Context, lsn uint64, source storage.SnapshotSource) error {
		return wal.WriteSnapshot(ctx, dir, lsn, source)
	}))
	require.False(t, store.Status().Degraded)
	files, err := filepath.Glob(filepath.Join(dir, "wal-*.log"))
	require.NoError(t, err)
	require.Empty(t, files)
}

type failingReader struct{ err error }

func (r failingReader) Read([]byte) (int, error) { return 0, r.err }

func TestRecordIOFailureIsNotPartialTail(t *testing.T) {
	t.Parallel()
	encoded, err := wal.EncodeRecord(wal.Record{LSN: 1, Command: wal.CommandSet, Args: []string{"t", "key", "value"}})
	require.NoError(t, err)
	failure := errors.New("injected I/O failure")
	for _, cut := range []int{0, 3, 10, len(encoded) - 2} {
		reader := bufio.NewReader(io.MultiReader(bytes.NewReader(encoded[:cut]), failingReader{err: failure}))
		_, err = wal.ReadRecord(reader)
		require.ErrorIs(t, err, failure)
		require.NotErrorIs(t, err, wal.ErrPartialRecord)
	}
}

func TestSnapshotFallbackUsesOlderVerifiedState(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	writer, err := wal.OpenWriter(wal.WriterConfig{Dir: dir, Sync: wal.SyncAlways, SegmentSize: 1}, 0)
	require.NoError(t, err)
	defer func() { require.NoError(t, writer.Close()) }()
	state := newTestState()
	state.set("t", "key", "first")
	_, err = writer.Append(t.Context(), wal.CommandSet, []string{"t", "key", "first"})
	require.NoError(t, err)
	require.NoError(t, wal.WriteSnapshot(t.Context(), dir, 1, state))
	require.NoError(t, writer.Prune(t.Context(), 1))
	_, oldPath, _, err := wal.LatestSnapshotInfo(dir)
	require.NoError(t, err)
	oldData, err := os.ReadFile(oldPath)
	require.NoError(t, err)
	state.set("t", "key", "second")
	_, err = writer.Append(t.Context(), wal.CommandSet, []string{"t", "key", "second"})
	require.NoError(t, err)
	require.NoError(t, wal.WriteSnapshot(t.Context(), dir, 2, state))
	_, newPath, _, err := wal.LatestSnapshotInfo(dir)
	require.NoError(t, err)
	// Simulate an older snapshot retained before cleanup, alongside a damaged newer publication.
	require.NoError(t, os.WriteFile(oldPath, oldData, 0o600))
	require.NoError(t, os.Truncate(newPath, 20))
	recovered := newTestState()
	lsn, err := wal.LoadLatestSnapshot(dir, func(table, key, value string) error { recovered.set(table, key, value); return nil })
	require.NoError(t, err)
	require.Equal(t, uint64(1), lsn)
	lsn, err = wal.NewReader(dir, nil).Replay(lsn, func(record wal.Record) error {
		recovered.set(record.Args[0], record.Args[1], record.Args[2])
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, uint64(2), lsn)
	require.Equal(t, state, recovered)
}

func TestSnapshotVersionAndFilenameLSNAreVerified(t *testing.T) {
	t.Parallel()
	t.Run("old version", func(t *testing.T) {
		t.Parallel()
		dir := t.TempDir()
		require.NoError(t, os.WriteFile(filepath.Join(dir, "snapshot-00000000000000000001.db"), []byte("DBSNP\x00\x02"), 0o600))
		_, err := wal.LoadLatestSnapshot(dir, func(_, _, _ string) error { t.Fatal("applied old snapshot"); return nil })
		require.ErrorIs(t, err, wal.ErrUnsupportedFormat)
	})
	t.Run("renamed snapshot", func(t *testing.T) {
		t.Parallel()
		dir := t.TempDir()
		require.NoError(t, wal.WriteSnapshot(t.Context(), dir, 1, newTestState()))
		_, path, _, err := wal.LatestSnapshotInfo(dir)
		require.NoError(t, err)
		require.NoError(t, os.Rename(path, filepath.Join(dir, "snapshot-00000000000000000002.db")))
		require.ErrorIs(t, wal.VerifySnapshot(dir, 2), wal.ErrInvalidSnapshot)
	})
}

func TestPruneWithoutSnapshotRetainsSegments(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	writer, err := wal.OpenWriter(wal.WriterConfig{Dir: dir, Sync: wal.SyncAlways, SegmentSize: 1}, 0)
	require.NoError(t, err)
	defer func() { require.NoError(t, writer.Close()) }()
	for range 3 {
		_, err = writer.Append(t.Context(), wal.CommandSet, []string{"t", "key", "value"})
		require.NoError(t, err)
	}
	before := walSegmentFiles(t, dir)
	require.Error(t, writer.Prune(t.Context(), 3))
	require.Equal(t, before, walSegmentFiles(t, dir))
	require.True(t, writer.Status().Ready)
	require.True(t, writer.Status().Degraded)
	_, err = writer.Append(t.Context(), wal.CommandSet, []string{"t", "key", "next"})
	require.NoError(t, err)
}

func TestPartialEarlierSegmentIsNeverRepaired(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	first, err := wal.EncodeRecord(wal.Record{LSN: 1, Command: wal.CommandSet, Args: []string{"t", "key", "value"}})
	require.NoError(t, err)
	path := filepath.Join(dir, "wal-00000000000000000001.log")
	data := append([]byte("DBWAL\x00\x02"), first[:8]...)
	require.NoError(t, os.WriteFile(path, data, 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "wal-00000000000000000002.log"), []byte("DBWAL\x00\x02"), 0o600))
	_, err = wal.NewReader(dir, nil).Replay(0, func(wal.Record) error { return nil })
	require.ErrorIs(t, err, wal.ErrPartialRecord)
	after, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, data, after)
}

func TestSnapshotVerificationFailurePreservesPreviousSnapshot(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	state := newTestState()
	state.set("t", "key", "value")
	require.NoError(t, wal.WriteSnapshot(t.Context(), dir, 1, state))
	_, path, _, err := wal.LatestSnapshotInfo(dir)
	require.NoError(t, err)
	before, err := os.ReadFile(path)
	require.NoError(t, err)
	err = wal.WriteSnapshotWithSync(t.Context(), dir, 2, state, func(file *os.File) error {
		info, statErr := file.Stat()
		if statErr != nil {
			return statErr
		}
		// Simulate a write that appeared successful but did not persist its completion marker.
		if truncateErr := file.Truncate(info.Size() - 1); truncateErr != nil {
			return truncateErr
		}
		return file.Sync()
	})
	require.ErrorIs(t, err, wal.ErrInvalidSnapshot)
	lsn, latestPath, _, err := wal.LatestSnapshotInfo(dir)
	require.NoError(t, err)
	require.Equal(t, uint64(1), lsn)
	require.Equal(t, path, latestPath)
	after, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, before, after)
}
