package tiered_test

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/OutOfStack/db/internal/engine/tiered"
	"github.com/OutOfStack/db/internal/wal"
	"github.com/stretchr/testify/require"
)

// TestCloseSyncsEverySegmentUnderSyncNo pins the clean-shutdown guarantee: SyncNo never fsyncs while the engine runs,
// yet Close makes every segment durable — sealed ones included, which were rotated without a sync — so a stop that
// exits 0 leaves nothing only in the page cache.
func TestCloseSyncsEverySegmentUnderSyncNo(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	cfg := testConfig(dir)
	cfg.Sync = wal.SyncNo
	cfg.SegmentSize = 64 // a few records per segment, so the writes below rotate several times

	var mu sync.Mutex
	synced := make(map[string]int)
	e, err := tiered.OpenWithSync(cfg, nil, func(f *os.File) error {
		mu.Lock()
		synced[filepath.Base(f.Name())]++
		mu.Unlock()
		return f.Sync()
	})
	require.NoError(t, err)
	for i := range 20 {
		require.NoError(t, e.Set(t.Context(), "t", fmt.Sprintf("key-%02d", i), "value"))
	}
	mu.Lock()
	require.Empty(t, synced, "SyncNo must not fsync while running")
	mu.Unlock()

	require.NoError(t, e.Close())

	segments, err := filepath.Glob(filepath.Join(dir, tiered.SegPrefix+"*"+tiered.SegSuffix))
	require.NoError(t, err)
	require.Greater(t, len(segments), 1, "the test needs sealed segments as well as the active one")
	for _, segment := range segments {
		require.Positive(t, synced[filepath.Base(segment)], "segment %s was not synced on close", segment)
	}
}

func TestCloseReturnsShutdownSyncFailure(t *testing.T) {
	t.Parallel()

	cfg := testConfig(t.TempDir())
	cfg.Sync = wal.SyncNo
	fail := errors.New("injected fsync failure")
	e, err := tiered.OpenWithSync(cfg, nil, func(*os.File) error { return fail })
	require.NoError(t, err)
	require.NoError(t, e.Set(t.Context(), "t", "k", "v"), "SyncNo does not fsync on write")

	closeErr := e.Close()
	require.ErrorIs(t, closeErr, fail)
	require.ErrorContains(t, closeErr, "during close")
}
