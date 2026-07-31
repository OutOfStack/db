package tiered_test

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/OutOfStack/db/internal/engine"
	"github.com/OutOfStack/db/internal/engine/tiered"
	"github.com/OutOfStack/db/internal/wal"
)

func testConfig(dir string) tiered.Config {
	return tiered.Config{
		Dir:                 dir,
		MaxMemoryBytes:      1 << 20,
		MaxStorageBytes:     1 << 20,
		SegmentSize:         1 << 20,
		Sync:                wal.SyncNo,
		CompactionThreshold: 0.5,
		CompactionInterval:  0, // background loop disabled; tests drive compaction directly
	}
}

func open(t *testing.T, cfg tiered.Config) *tiered.Engine {
	t.Helper()
	e, err := tiered.Open(cfg, nil)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = e.Close() })
	return e
}

func mustGet(t *testing.T, e *tiered.Engine, table, key string) string {
	t.Helper()
	value, err := e.Get(context.Background(), table, key)
	if err != nil {
		t.Fatalf("get %s/%s: %v", table, key, err)
	}
	return value
}

func TestSetGetDelOverwrite(t *testing.T) {
	e := open(t, testConfig(t.TempDir()))
	ctx := context.Background()

	if _, err := e.Get(ctx, "t", "missing"); !errors.Is(err, engine.ErrNotFound) {
		t.Fatalf("want ErrNotFound, got %v", err)
	}
	if err := e.Set(ctx, "t", "k", "v1"); err != nil {
		t.Fatal(err)
	}
	if got := mustGet(t, e, "t", "k"); got != "v1" {
		t.Fatalf("got %q", got)
	}
	if err := e.Set(ctx, "t", "k", "v2"); err != nil {
		t.Fatal(err)
	}
	if got := mustGet(t, e, "t", "k"); got != "v2" {
		t.Fatalf("overwrite: got %q", got)
	}
	if err := e.Del(ctx, "t", "k"); err != nil {
		t.Fatal(err)
	}
	if _, err := e.Get(ctx, "t", "k"); !errors.Is(err, engine.ErrNotFound) {
		t.Fatalf("after del want ErrNotFound, got %v", err)
	}
	if err := e.Del(ctx, "t", "k"); !errors.Is(err, engine.ErrNotFound) {
		t.Fatalf("del missing want ErrNotFound, got %v", err)
	}
}

func TestIntrospection(t *testing.T) {
	e := open(t, testConfig(t.TempDir()))
	ctx := context.Background()
	_ = e.Set(ctx, "users", "a", "1")
	_ = e.Set(ctx, "users", "b", "2")
	_ = e.Set(ctx, "orders", "x", "9")

	if tables := e.Tables(ctx); len(tables) != 2 || tables[0] != "orders" || tables[1] != "users" {
		t.Fatalf("tables: %v", tables)
	}
	if !e.TableExists(ctx, "users") || e.TableExists(ctx, "nope") {
		t.Fatal("table existence wrong")
	}
	if keys := e.Keys(ctx, "users"); len(keys) != 2 || keys[0] != "a" || keys[1] != "b" {
		t.Fatalf("keys: %v", keys)
	}
	// Deleting the last key of a table drops the table.
	_ = e.Del(ctx, "orders", "x")
	if e.TableExists(ctx, "orders") {
		t.Fatal("empty table should be gone")
	}
}

// TestEvictionServesFromDisk forces constant eviction with a tiny memory budget
// and verifies every value is still readable (from disk on a miss).
func TestEvictionServesFromDisk(t *testing.T) {
	cfg := testConfig(t.TempDir())
	cfg.MaxMemoryBytes = 64 // only ~1 value fits in RAM at a time
	e := open(t, cfg)
	ctx := context.Background()

	const n = 200
	for i := range n {
		if err := e.Set(ctx, "t", fmt.Sprintf("k%03d", i), fmt.Sprintf("value-%d", i)); err != nil {
			t.Fatal(err)
		}
	}
	for i := range n {
		want := fmt.Sprintf("value-%d", i)
		if got := mustGet(t, e, "t", fmt.Sprintf("k%03d", i)); got != want {
			t.Fatalf("k%03d: got %q want %q", i, got, want)
		}
	}
	if s := e.Stats(); s.Misses == 0 {
		t.Fatal("expected cache misses with tiny memory budget")
	}
}

func TestStorageFullThenDeleteResumes(t *testing.T) {
	cfg := testConfig(t.TempDir())
	cfg.MaxStorageBytes = 2000
	e := open(t, cfg)
	ctx := context.Background()

	value := make([]byte, 100)
	var lastKey string
	full := false
	for i := range 100 {
		key := fmt.Sprintf("k%03d", i)
		err := e.Set(ctx, "t", key, string(value))
		if errors.Is(err, tiered.ErrStorageFull) {
			full = true
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		lastKey = key
	}
	if !full {
		t.Fatal("expected ErrStorageFull before 100 keys")
	}
	// Freeing a live key must let a new write through (live-byte accounting).
	if err := e.Del(ctx, "t", lastKey); err != nil {
		t.Fatal(err)
	}
	if err := e.Set(ctx, "t", "after-delete", string(value)); err != nil {
		t.Fatalf("write should resume after delete: %v", err)
	}
}

func TestRecovery(t *testing.T) {
	dir := t.TempDir()
	cfg := testConfig(dir)
	ctx := context.Background()

	e, err := tiered.Open(cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	_ = e.Set(ctx, "t", "keep", "final")
	_ = e.Set(ctx, "t", "keep", "overwritten-then-final")
	_ = e.Set(ctx, "t", "keep", "final")
	_ = e.Set(ctx, "t", "gone", "x")
	_ = e.Del(ctx, "t", "gone")
	_ = e.Set(ctx, "other", "y", "z")
	if err = e.Close(); err != nil {
		t.Fatal(err)
	}

	e2 := open(t, cfg)
	if got := mustGet(t, e2, "t", "keep"); got != "final" {
		t.Fatalf("recovered value: %q", got)
	}
	if _, gerr := e2.Get(ctx, "t", "gone"); !errors.Is(gerr, engine.ErrNotFound) {
		t.Fatalf("deleted key recovered: %v", gerr)
	}
	if got := mustGet(t, e2, "other", "y"); got != "z" {
		t.Fatalf("other table: %q", got)
	}
}

// TestCompactionReclaimsDisk overwrites one key across many sealed segments,
// then compacts and checks the dataset is intact and disk usage dropped.
func TestCompactionReclaimsDisk(t *testing.T) {
	cfg := testConfig(t.TempDir())
	cfg.SegmentSize = 256 // tiny segments so overwrites roll and seal quickly
	e := open(t, cfg)
	ctx := context.Background()

	value := "0123456789012345678901234567890123456789" // 40 bytes
	for i := range 50 {
		if err := e.Set(ctx, "t", "hot", fmt.Sprintf("%s-%d", value, i)); err != nil {
			t.Fatal(err)
		}
	}
	before := e.Stats()
	if before.Segments < 3 {
		t.Fatalf("expected several segments, got %d", before.Segments)
	}

	// Reclaim every eligible sealed segment.
	for range before.Segments {
		e.Compact()
	}
	after := e.Stats()

	if got := mustGet(t, e, "t", "hot"); got != fmt.Sprintf("%s-%d", value, 49) {
		t.Fatalf("value after compaction: %q", got)
	}
	if after.DiskBytes >= before.DiskBytes {
		t.Fatalf("disk not reclaimed: before=%d after=%d", before.DiskBytes, after.DiskBytes)
	}
	if after.Compactions == 0 {
		t.Fatal("expected at least one compaction")
	}
}

// TestRestartAfterCompaction reopens the data directory without closing the
// engine (a crash right after a compaction pass): the rewritten records must be
// on disk, since compaction already deleted the segment they came from.
func TestRestartAfterCompaction(t *testing.T) {
	dir := t.TempDir()
	cfg := testConfig(dir)
	cfg.SegmentSize = 256
	ctx := context.Background()

	e, err := tiered.Open(cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	// Closed only at teardown, after the reopened engine has been asserted, so
	// its sync cannot stand in for the one compaction owes.
	t.Cleanup(func() { _ = e.Close() })
	for i := range 20 {
		if err = e.Set(ctx, "t", fmt.Sprintf("k%02d", i), fmt.Sprintf("value-%02d", i)); err != nil {
			t.Fatal(err)
		}
	}
	for i := range 20 { // overwrite everything so the old segments go mostly dead
		if err = e.Set(ctx, "t", fmt.Sprintf("k%02d", i), fmt.Sprintf("final-%02d", i)); err != nil {
			t.Fatal(err)
		}
	}
	for range e.Stats().Segments {
		e.Compact()
	}
	if e.Stats().Compactions == 0 {
		t.Fatal("expected at least one compaction")
	}

	e2 := open(t, cfg)
	for i := range 20 {
		key := fmt.Sprintf("k%02d", i)
		if got := mustGet(t, e2, "t", key); got != fmt.Sprintf("final-%02d", i) {
			t.Fatalf("%s after restart: %q", key, got)
		}
	}
}

// TestOversizedValueStaysDiskOnly checks max_memory is a real ceiling: a value
// bigger than the whole budget is served from disk, never cached.
func TestOversizedValueStaysDiskOnly(t *testing.T) {
	cfg := testConfig(t.TempDir())
	cfg.MaxMemoryBytes = 128
	e := open(t, cfg)
	ctx := context.Background()

	big := string(make([]byte, 4096))
	if err := e.Set(ctx, "t", "big", big); err != nil {
		t.Fatal(err)
	}
	before := e.Stats().Misses
	if got := mustGet(t, e, "t", "big"); got != big {
		t.Fatalf("oversized value: got %d bytes", len(got))
	}
	if e.Stats().Misses == before {
		t.Fatal("oversized value was cached; max_memory is not a ceiling")
	}
	// A small value still caches, and an oversized overwrite must not leave it stale.
	if err := e.Set(ctx, "t", "k", "small"); err != nil {
		t.Fatal(err)
	}
	if err := e.Set(ctx, "t", "k", big); err != nil {
		t.Fatal(err)
	}
	if got := mustGet(t, e, "t", "k"); got != big {
		t.Fatalf("stale cached value after oversized overwrite: got %d bytes", len(got))
	}
}

// TestParityWithInMemory runs the same random workload against the tiered engine
// (with constant eviction) and the plain in-memory engine; GET results must match.
func TestParityWithInMemory(t *testing.T) {
	cfg := testConfig(t.TempDir())
	cfg.MaxMemoryBytes = 128
	cfg.SegmentSize = 512
	tieredEngine := open(t, cfg)
	memEngine := engine.New()
	ctx := context.Background()

	rng := rand.New(rand.NewSource(1))
	keys := []string{"a", "b", "c", "d", "e", "f", "g", "h"}
	for range 5000 {
		key := keys[rng.Intn(len(keys))]
		switch rng.Intn(3) {
		case 0, 1:
			value := fmt.Sprintf("v-%d", rng.Intn(1000))
			if err := tieredEngine.Set(ctx, "t", key, value); err != nil {
				t.Fatal(err)
			}
			_ = memEngine.Set(ctx, "t", key, value)
		default:
			terr := tieredEngine.Del(ctx, "t", key)
			merr := memEngine.Del(ctx, "t", key)
			if errors.Is(terr, engine.ErrNotFound) != errors.Is(merr, engine.ErrNotFound) {
				t.Fatalf("del mismatch for %s: tiered=%v mem=%v", key, terr, merr)
			}
		}
		if rng.Intn(20) == 0 {
			tieredEngine.Compact()
		}
		for _, k := range keys {
			assertSameGet(t, tieredEngine, memEngine, k)
		}
	}
}

func assertSameGet(t *testing.T, tieredEngine *tiered.Engine, memEngine *engine.Engine, key string) {
	t.Helper()
	ctx := context.Background()
	tv, terr := tieredEngine.Get(ctx, "t", key)
	mv, merr := memEngine.Get(ctx, "t", key)
	if errors.Is(terr, engine.ErrNotFound) != errors.Is(merr, engine.ErrNotFound) {
		t.Fatalf("presence mismatch for %s: tiered=%v mem=%v", key, terr, merr)
	}
	if terr == nil && tv != mv {
		t.Fatalf("value mismatch for %s: tiered=%q mem=%q", key, tv, mv)
	}
}

// TestRecoveryTruncatesTornTail simulates a crash mid-write: garbage bytes are
// appended to the last segment. Recovery must truncate them and keep prior data.
func TestRecoveryTruncatesTornTail(t *testing.T) {
	dir := t.TempDir()
	cfg := testConfig(dir)
	ctx := context.Background()

	e, err := tiered.Open(cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	_ = e.Set(ctx, "t", "a", "1")
	_ = e.Set(ctx, "t", "b", "2")
	if err = e.Close(); err != nil {
		t.Fatal(err)
	}

	// Append a partial record (torn header) to the newest segment.
	segPath := lastSegment(t, dir)
	file, err := os.OpenFile(segPath, os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = file.Write([]byte{0xDE, 0xAD, 0xBE}); err != nil {
		t.Fatal(err)
	}
	_ = file.Close()

	e2 := open(t, cfg)
	if got := mustGet(t, e2, "t", "a"); got != "1" {
		t.Fatalf("a after torn tail: %q", got)
	}
	if got := mustGet(t, e2, "t", "b"); got != "2" {
		t.Fatalf("b after torn tail: %q", got)
	}
	// The truncated tail must not corrupt subsequent appends.
	if err = e2.Set(ctx, "t", "c", "3"); err != nil {
		t.Fatalf("append after recovery: %v", err)
	}
	if got := mustGet(t, e2, "t", "c"); got != "3" {
		t.Fatalf("c: %q", got)
	}
}

func lastSegment(t *testing.T, dir string) string {
	t.Helper()
	matches, err := filepath.Glob(filepath.Join(dir, "seg-*.data"))
	if err != nil || len(matches) == 0 {
		t.Fatalf("no segments in %s: %v", dir, err)
	}
	slices.Sort(matches)
	return matches[len(matches)-1]
}

// TestConcurrentWritesAndCompaction exercises writes racing compaction under the
// engine lock. Run with -race in CI; here it asserts final-state correctness.
func TestConcurrentWritesAndCompaction(t *testing.T) {
	cfg := testConfig(t.TempDir())
	cfg.SegmentSize = 512
	e := open(t, cfg)
	ctx := context.Background()

	const writers, perWriter = 4, 250
	var wg sync.WaitGroup
	for w := range writers {
		wg.Go(func() {
			for i := range perWriter {
				key := fmt.Sprintf("w%d-k%d", w, i)
				if err := e.Set(ctx, "t", key, fmt.Sprintf("v%d", i)); err != nil {
					t.Errorf("set: %v", err)
					return
				}
			}
		})
	}
	wg.Go(func() {
		for range 100 {
			e.Compact()
			time.Sleep(time.Millisecond)
		}
	})
	wg.Wait()

	for w := range writers {
		for i := range perWriter {
			key := fmt.Sprintf("w%d-k%d", w, i)
			if got := mustGet(t, e, "t", key); got != fmt.Sprintf("v%d", i) {
				t.Fatalf("%s: got %q", key, got)
			}
		}
	}
}

func TestBackgroundLoopsStartAndStop(t *testing.T) {
	cfg := testConfig(t.TempDir())
	cfg.Sync = wal.SyncEverySec
	cfg.CompactionInterval = 10 * time.Millisecond
	e, err := tiered.Open(cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	for i := range 100 {
		_ = e.Set(ctx, "t", fmt.Sprintf("k%d", i), "v")
	}
	time.Sleep(30 * time.Millisecond) // let the loops run at least once
	if err = e.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
}
