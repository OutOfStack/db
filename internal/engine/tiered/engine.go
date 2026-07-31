// Package tiered implements a durable key-value engine whose dataset can exceed
// RAM. It is a Bitcask-style store: an append-only set of on-disk segments is the
// source of truth, a full in-memory keydir maps every live (table,key) to its
// on-disk location, and an LRU cache keeps hot values in memory. Evicting a cached
// value is free and lossless because the value is already durable on disk.
//
// The engine is self-contained: it provides its own durability (fsync of its
// segments) and needs no write-ahead log — it borrows only wal.SyncPolicy, to
// spell the fsync policy the same way the WAL-backed engine does. It implements
// the storage.Engine interface, so the server selects it via engine.type=tiered
// instead of the RAM-only engine.
package tiered

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"slices"
	"sync"
	"time"

	"github.com/OutOfStack/db/internal/engine"
	"github.com/OutOfStack/db/internal/wal"
)

// ErrStorageFull is returned by Set when the live dataset would exceed the
// configured storage limit. It surfaces to clients as "ERR storage full".
var ErrStorageFull = errors.New("storage full")

// Config configures a tiered engine.
type Config struct {
	Dir                 string
	MaxMemoryBytes      int64 // hot values kept in RAM (LRU budget)
	MaxStorageBytes     int64 // live dataset ceiling; 0 disables the limit
	SegmentSize         int64
	Sync                wal.SyncPolicy
	CompactionThreshold float64       // reclaim a sealed segment past this dead-bytes ratio
	CompactionInterval  time.Duration // how often to check for compaction and log stats
}

// loc is a keydir entry: where a live value lives on disk and the record's size.
type loc struct {
	seg     uint32
	valPos  int64
	valLen  uint32
	recSize int64
}

// Engine is a tiered (memory + disk) storage engine.
type Engine struct {
	mu     sync.Mutex
	store  *store
	keydir map[string]map[string]loc // table -> key -> location
	lru    *lruCache
	logger *slog.Logger

	liveBytes int64
	segLive   map[uint32]int64 // live bytes per segment (for compaction & dead-ratio)
	maxStore  int64
	threshold float64

	hits, misses, compactions uint64

	done chan struct{}
	wg   sync.WaitGroup
}

// Open recovers the keydir from the on-disk segments and starts the background
// sync (everysec) and compaction loops.
func Open(cfg Config, logger *slog.Logger) (*Engine, error) {
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	st, err := openStore(cfg.Dir, cfg.SegmentSize, cfg.Sync)
	if err != nil {
		return nil, err
	}
	e := &Engine{
		store:     st,
		keydir:    make(map[string]map[string]loc),
		lru:       newLRU(cfg.MaxMemoryBytes),
		logger:    logger,
		segLive:   make(map[uint32]int64),
		maxStore:  cfg.MaxStorageBytes,
		threshold: cfg.CompactionThreshold,
		done:      make(chan struct{}),
	}
	if err = e.recover(); err != nil {
		_ = st.close()
		return nil, err
	}
	logger.Info("Tiered engine recovered", "keys", e.keyCount(), "live_bytes", e.liveBytes)

	if cfg.Sync == wal.SyncEverySec {
		e.wg.Go(e.syncLoop)
	}
	if interval := cfg.CompactionInterval; interval > 0 {
		e.wg.Go(func() { e.maintenanceLoop(interval) })
	}
	return e, nil
}

// Close stops background loops, flushes, and closes the segments.
func (e *Engine) Close() error {
	close(e.done)
	e.wg.Wait()
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.store.close()
}

func (e *Engine) recover() error {
	segs := e.store.segments()
	for i, seg := range segs {
		isLast := i == len(segs)-1
		// Later records win, so an overwrite or tombstone supersedes the earlier
		// value. Only the newest segment may carry a torn tail from a crash.
		if err := e.store.scanSegment(seg, isLast, func(rec decoded, recPos int64) {
			e.dropLive(rec.table, rec.key)
			if !rec.tombstone {
				e.setLoc(seg, rec.table, rec.key, len(rec.value), recPos, rec.recSize)
			}
		}); err != nil {
			return err
		}
	}
	return nil
}

// dropLive removes a key's keydir entry and its live-byte accounting, if present.
func (e *Engine) dropLive(table, key string) {
	keys, ok := e.keydir[table]
	if !ok {
		return
	}
	old, ok := keys[key]
	if !ok {
		return
	}
	e.liveBytes -= old.recSize
	e.segLive[old.seg] -= old.recSize
	delete(keys, key)
	if len(keys) == 0 {
		delete(e.keydir, table)
	}
}

// setLoc records a live value's location and adds its live-byte accounting.
// Callers pass valLen rather than the value itself: recovery never needs the
// bytes, only their length.
func (e *Engine) setLoc(seg uint32, table, key string, valLen int, recPos, recSize int64) {
	keys, ok := e.keydir[table]
	if !ok {
		keys = make(map[string]loc)
		e.keydir[table] = keys
	}
	keys[key] = loc{
		seg:     seg,
		valPos:  valPosFor(recPos, table, key),
		valLen:  u32(valLen),
		recSize: recSize,
	}
	e.liveBytes += recSize
	e.segLive[seg] += recSize
}

// Set appends the value and updates the keydir and cache. It rejects the write
// with ErrStorageFull if the live dataset would exceed the configured limit.
func (e *Engine) Set(_ context.Context, tbl, key, value string) error {
	if len(tbl) > maxFieldLen || len(key) > maxFieldLen {
		return fmt.Errorf("table/key exceeds %d bytes", maxFieldLen)
	}
	if len(value) > maxValueLen {
		return fmt.Errorf("value exceeds %d bytes", maxValueLen)
	}
	rec := encodeRecord(tbl, key, value, false)
	recSize := int64(len(rec))

	e.mu.Lock()
	defer e.mu.Unlock()

	old, exists := e.lookup(tbl, key)
	newLive := e.liveBytes + recSize
	if exists {
		newLive -= old.recSize
	}
	if e.maxStore > 0 && newLive > e.maxStore {
		return ErrStorageFull
	}

	seg, recPos, err := e.store.append(rec)
	if err != nil {
		return err
	}
	e.dropLive(tbl, key)
	e.setLoc(seg, tbl, key, len(value), recPos, recSize)
	e.lru.put(tbl, key, value)
	return e.store.syncIfAlways()
}

// Get returns a value, populating the cache on a miss by reading from disk.
//
// ponytail: the cache-miss pread happens under the engine mutex, so a slow disk
// serializes every other operation behind it. Dropping the lock around the read
// needs the segment pinned against concurrent compaction; do that if read
// latency under a cold cache ever matters.
func (e *Engine) Get(_ context.Context, tbl, key string) (string, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	location, ok := e.lookup(tbl, key)
	if !ok {
		return "", engine.ErrNotFound
	}
	if value, hit := e.lru.get(tbl, key); hit {
		e.hits++
		return value, nil
	}
	e.misses++
	value, err := e.store.readValue(location.seg, location.valPos, location.valLen)
	if err != nil {
		return "", err
	}
	e.lru.put(tbl, key, value)
	return value, nil
}

// Del appends a tombstone and removes the key from the keydir and cache.
func (e *Engine) Del(_ context.Context, tbl, key string) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if _, ok := e.lookup(tbl, key); !ok {
		return engine.ErrNotFound
	}
	if _, _, err := e.store.append(encodeRecord(tbl, key, "", true)); err != nil {
		return err
	}
	e.dropLive(tbl, key)
	e.lru.remove(tbl, key)
	return e.store.syncIfAlways()
}

// Tables returns all table names in sorted order.
func (e *Engine) Tables(_ context.Context) []string {
	e.mu.Lock()
	defer e.mu.Unlock()
	return slices.Sorted(maps.Keys(e.keydir))
}

// TableExists reports whether a table has at least one live key.
func (e *Engine) TableExists(_ context.Context, tbl string) bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	_, ok := e.keydir[tbl]
	return ok
}

// Keys returns all keys in a table in sorted order.
func (e *Engine) Keys(_ context.Context, tbl string) []string {
	e.mu.Lock()
	defer e.mu.Unlock()
	return slices.Sorted(maps.Keys(e.keydir[tbl]))
}

// Range calls fn for every live value, reading from disk on a cache miss.
func (e *Engine) Range(fn func(table, key, value string) bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	for tbl, keys := range e.keydir {
		for key, location := range keys {
			value, hit := e.lru.get(tbl, key)
			if !hit {
				var err error
				value, err = e.store.readValue(location.seg, location.valPos, location.valLen)
				if err != nil {
					e.logger.Error("Range read failed", "table", tbl, "key", key, "error", err)
					continue
				}
			}
			if !fn(tbl, key, value) {
				return
			}
		}
	}
}

// Replace swaps all state for a replication resync snapshot — unreachable here:
// it is called only by a standby, and the config rejects tiered alongside
// replication. It stays a stub rather than untested machinery; wiring the two
// together needs a shared LSN-tagged log, not this method (see docs/plans/05).
func (e *Engine) Replace([]engine.Entry) {
	e.logger.Error("Replace is not supported by the tiered engine")
}

func (e *Engine) lookup(tbl, key string) (loc, bool) {
	location, ok := e.keydir[tbl][key]
	return location, ok
}

func (e *Engine) keyCount() int {
	count := 0
	for _, keys := range e.keydir {
		count += len(keys)
	}
	return count
}

func (e *Engine) syncLoop() {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-e.done:
			return
		case <-ticker.C:
			e.mu.Lock()
			if err := e.store.syncActive(); err != nil {
				e.logger.Error("Tiered sync failed", "error", err)
			}
			e.mu.Unlock()
		}
	}
}
