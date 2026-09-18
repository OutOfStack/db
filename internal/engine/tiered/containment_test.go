package tiered_test

import (
	"context"
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/OutOfStack/db/internal/engine/tiered"
	"github.com/OutOfStack/db/internal/wal"
	"github.com/stretchr/testify/require"
)

// TestFsyncFailureIsTerminal injects an fsync failure into the everysec sync loop and verifies the engine latches it:
// later mutations are refused with ErrTerminal, Status reports not ready, and Close returns the error.
func TestFsyncFailureIsTerminal(t *testing.T) {
	t.Parallel()
	cfg := testConfig(t.TempDir())
	cfg.Sync = wal.SyncEverySec
	fail := errors.New("injected fsync failure")
	calls := 0
	e, err := tiered.OpenWithSync(cfg, nil, func(f *os.File) error {
		calls++
		if calls > 1 { // let the initial segment header sync through, fail the ticker's first sync
			return fail
		}
		return f.Sync()
	})
	require.NoError(t, err)
	ctx := context.Background()
	require.NoError(t, e.Set(ctx, "t", "before", "v"))
	require.True(t, e.Status().Ready)

	require.Eventually(t, func() bool { return !e.Status().Ready }, 5*time.Second, 10*time.Millisecond,
		"sync loop never latched the failure")
	status := e.Status()
	require.True(t, status.Degraded)
	require.ErrorIs(t, status.TerminalError, tiered.ErrTerminal)
	require.ErrorIs(t, status.TerminalError, fail)

	require.ErrorIs(t, e.Set(ctx, "t", "after", "v"), tiered.ErrTerminal)
	require.ErrorIs(t, e.Del(ctx, "t", "before"), tiered.ErrTerminal)
	require.ErrorIs(t, e.Update(ctx, "t", "before", func(string, bool) (string, error) { return "x", nil }),
		tiered.ErrTerminal)
	// Reads of what the engine already holds keep working.
	require.Equal(t, "v", mustGet(t, e, "t", "before"))

	closeErr := e.Close()
	require.ErrorIs(t, closeErr, tiered.ErrTerminal)
	require.ErrorIs(t, closeErr, fail)
}

// TestFsyncFailureUnderAlwaysIsTerminal covers the per-write path: under the always policy the failing fsync surfaces
// on the write itself and latches the same way.
func TestFsyncFailureUnderAlwaysIsTerminal(t *testing.T) {
	t.Parallel()
	cfg := testConfig(t.TempDir())
	cfg.Sync = wal.SyncAlways
	calls := 0
	e, err := tiered.OpenWithSync(cfg, nil, func(f *os.File) error {
		calls++
		if calls > 1 { // let the initial segment header sync through, fail the first record sync
			return errors.New("disk gone")
		}
		return f.Sync()
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = e.Close() })
	ctx := context.Background()
	require.ErrorIs(t, e.Set(ctx, "t", "k", "v"), tiered.ErrTerminal)
	require.ErrorIs(t, e.Set(ctx, "t", "k2", "v"), tiered.ErrTerminal)
	require.False(t, e.Status().Ready)
}

// flipByte flips one byte of a file in place.
func flipByte(t *testing.T, path string, offset int64) {
	t.Helper()
	file, err := os.OpenFile(path, os.O_RDWR, 0o600)
	require.NoError(t, err)
	defer func() { require.NoError(t, file.Close()) }()
	b := make([]byte, 1)
	_, err = file.ReadAt(b, offset)
	require.NoError(t, err)
	b[0] ^= 0xFF
	_, err = file.WriteAt(b, offset)
	require.NoError(t, err)
}

const segmentHeaderLen = 7

// TestBitFlipFailsColdReadAndRecovery flips a byte inside a stored value. The cold read of that key fails with a
// corruption error while other keys stay readable, and reopening the directory refuses to start: a checksum mismatch is
// never truncated as a crash tail, even in the newest segment.
func TestBitFlipFailsColdReadAndRecovery(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	cfg := testConfig(dir)
	cfg.MaxMemoryBytes = 32 // nothing stays cached, so every read goes to disk
	ctx := context.Background()
	e, err := tiered.Open(cfg, nil)
	require.NoError(t, err)
	const value = "0123456789abcdef0123456789abcdef"
	require.NoError(t, e.Set(ctx, "t", "victim", value))
	require.NoError(t, e.Set(ctx, "t", "fine", "ok"))
	// The first record starts right after the segment header; its value starts after the 8-byte header plus table+key.
	flipByte(t, lastSegment(t, dir), segmentHeaderLen+8+int64(len("t")+len("victim"))+5)

	_, err = e.Get(ctx, "t", "victim")
	require.ErrorContains(t, err, "corrupt segment 1 at offset 7")
	require.ErrorContains(t, err, "checksum mismatch")
	require.Equal(t, "ok", mustGet(t, e, "t", "fine"))
	// The keydir is untouched: the key is still present, just unreadable.
	require.Equal(t, []string{"fine", "victim"}, e.Keys(ctx, "t"))
	require.True(t, e.Status().Ready, "a corrupt read must not stop the engine")
	require.NoError(t, e.Close())

	_, err = tiered.Open(cfg, nil)
	require.ErrorContains(t, err, "corrupt segment 1 at offset 7")
	require.ErrorContains(t, err, "checksum mismatch")
	// Nothing was truncated.
	info, statErr := os.Stat(lastSegment(t, dir))
	require.NoError(t, statErr)
	require.Greater(t, info.Size(), int64(segmentHeaderLen+8+len("t")+len("victim")+len(value)+4))
}

// TestOversizedLengthIsBoundedBeforeAllocation corrupts the first record's value-length field to claim gigabytes, in a
// sealed segment and in the newest one. Both refuse to start without allocating the claimed size and without touching
// the file: the intact records behind the corrupt length are exactly what truncation would have deleted.
func TestOversizedLengthIsBoundedBeforeAllocation(t *testing.T) {
	t.Parallel()
	for _, sealed := range []bool{false, true} {
		t.Run(map[bool]string{false: "newest segment", true: "sealed segment"}[sealed], func(t *testing.T) {
			t.Parallel()
			dir := t.TempDir()
			cfg := testConfig(dir)
			cfg.SegmentSize = 128 // fits the corrupt record and the intact one behind it; a third rolls over
			ctx := context.Background()
			e, err := tiered.Open(cfg, nil)
			require.NoError(t, err)
			require.NoError(t, e.Set(ctx, "t", "a", strings.Repeat("x", 40)))
			require.NoError(t, e.Set(ctx, "t", "intact", "after the corrupt record"))
			if sealed {
				for e.Stats().Segments < 2 {
					require.NoError(t, e.Set(ctx, "t", "b", strings.Repeat("y", 40)))
				}
			}
			require.Equal(t, sealed, e.Stats().Segments > 1)
			require.NoError(t, e.Close())

			matches, err := filepath.Glob(filepath.Join(dir, "seg-*.data"))
			require.NoError(t, err)
			slices.Sort(matches)
			first := matches[0]
			before, err := os.ReadFile(first)
			require.NoError(t, err)
			file, err := os.OpenFile(first, os.O_RDWR, 0o600)
			require.NoError(t, err)
			huge := make([]byte, 4)
			binary.BigEndian.PutUint32(huge, 0xFFFFFFF0) // just below the tombstone marker: ~4 GiB
			_, err = file.WriteAt(huge, segmentHeaderLen+4)
			require.NoError(t, err)
			require.NoError(t, file.Close())

			done := make(chan struct{})
			var openErr error
			go func() {
				defer close(done)
				var reopened *tiered.Engine
				if reopened, openErr = tiered.Open(cfg, nil); openErr == nil {
					_ = reopened.Close()
				}
			}()
			select {
			case <-done:
			case <-time.After(10 * time.Second):
				t.Fatal("open did not return promptly; a corrupt length may have been allocated")
			}
			require.ErrorContains(t, openErr, "corrupt segment 1 at offset 7")
			require.ErrorContains(t, openErr, "claims")
			require.ErrorContains(t, openErr, "truncate the segment at the record's offset")
			after, err := os.ReadFile(first)
			require.NoError(t, err)
			copy(before[segmentHeaderLen+4:], huge) // the only bytes that may differ are the ones the test flipped
			require.Equal(t, before, after, "recovery modified a segment it reported as corrupt")
		})
	}
}

// TestSegmentHeaderFsyncFailureIsTerminal fails the fsync of a freshly rotated segment's header while the active
// segment's own syncs succeed, and verifies the rotation path latches like every other fsync path.
func TestSegmentHeaderFsyncFailureIsTerminal(t *testing.T) {
	t.Parallel()
	cfg := testConfig(t.TempDir())
	cfg.Sync = wal.SyncAlways
	cfg.SegmentSize = 64
	fail := errors.New("header fsync failed")
	e, err := tiered.OpenWithSync(cfg, nil, func(f *os.File) error {
		if strings.HasSuffix(f.Name(), "seg-0000000002.data") {
			return fail
		}
		return f.Sync()
	})
	require.NoError(t, err)
	ctx := context.Background()
	require.NoError(t, e.Set(ctx, "t", "a", strings.Repeat("x", 40)))
	err = e.Set(ctx, "t", "b", strings.Repeat("y", 40)) // overflows segment 1, so this rotates
	require.ErrorIs(t, err, tiered.ErrTerminal)
	require.ErrorIs(t, err, fail)
	require.False(t, e.Status().Ready)
	require.ErrorIs(t, e.Set(ctx, "t", "c", "v"), tiered.ErrTerminal)
	require.Equal(t, strings.Repeat("x", 40), mustGet(t, e, "t", "a"))
	require.ErrorIs(t, e.Close(), fail)
}

// TestCompactionFailureIsDegradedNotFatal corrupts a sealed segment after open so compaction's scan hits it: the pass
// fails, Status reports degraded with the maintenance error, but the engine keeps serving and accepting writes.
func TestCompactionFailureIsDegradedNotFatal(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	cfg := testConfig(dir)
	cfg.SegmentSize = 64
	cfg.CompactionThreshold = 0.1
	ctx := context.Background()
	e := open(t, cfg)
	for range 6 {
		require.NoError(t, e.Set(ctx, "t", "hot", strings.Repeat("v", 40)))
	}
	require.GreaterOrEqual(t, e.Stats().Segments, 2)
	matches, err := filepath.Glob(filepath.Join(dir, "seg-*.data"))
	require.NoError(t, err)
	slices.Sort(matches)
	flipByte(t, matches[0], segmentHeaderLen+8+3)

	e.Compact()
	status := e.Status()
	require.True(t, status.Ready)
	require.True(t, status.Degraded)
	require.ErrorContains(t, status.MaintenanceError, "corrupt segment 1")
	require.Zero(t, e.Stats().Compactions)
	require.NoError(t, e.Set(ctx, "t", "still", "writable"))
	require.Equal(t, "writable", mustGet(t, e, "t", "still"))
}
