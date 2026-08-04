// Package tiered implements a durable key-value engine whose dataset can exceed RAM. It is a Bitcask-style store: an
// append-only set of on-disk segments is the source of truth, a full in-memory keydir maps every live (table,key) to
// its on-disk location, and an LRU cache keeps hot values in memory. Evicting a cached value is free and lossless
// because the value is already durable on disk.
//
// The engine is self-contained: it provides its own durability (fsync of its segments) and needs no write-ahead log —
// it borrows only wal.SyncPolicy, to spell the fsync policy the same way the WAL-backed engine does. It implements the
// storage.Engine interface, so the server selects it via engine.type=tiered instead of the RAM-only engine.
package tiered

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/OutOfStack/db/internal/engine"
	"github.com/OutOfStack/db/internal/wal"
)

// ErrStorageFull is returned by Set when the live dataset would exceed the configured storage limit. It surfaces to
// clients as "ERR storage full".
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

// keyID identifies a key by a 64-bit FNV-1a hash of its table and key rather than by the strings themselves: the
// tombstone index below holds an entry per key per segment, and this is the engine whose whole purpose is a dataset
// larger than RAM. A hash collision makes keepTombstone believe an older SET exists and keep a delete longer than
// necessary — the behavior this index replaced — so collisions cost disk, never correctness.
type keyID uint64

func hashKey(table, key string) keyID {
	const (
		offset64 = 14695981039346656037
		prime64  = 1099511628211
	)
	h := uint64(offset64)
	for i := range len(table) {
		h = (h ^ uint64(table[i])) * prime64
	}
	h = (h ^ 0xff) * prime64 // separator, so ("ab","c") and ("a","bc") differ
	for i := range len(key) {
		h = (h ^ uint64(key[i])) * prime64
	}
	return keyID(h)
}

// Engine is a tiered (memory + disk) storage engine.
type Engine struct {
	// mu guards the keydir, byte accounting, and store metadata.
	mu     sync.RWMutex
	store  *store
	keydir map[string]map[string]loc // table -> key -> location
	lru    *lruCache
	logger *slog.Logger

	liveBytes int64
	segLive   map[uint32]int64 // live bytes per segment (for compaction & dead-ratio)
	// segSets records which keys have a SET record in each segment, which lets a tombstone be dropped as soon as no older
	// segment can contradict it. A segment's entry is reclaimed when it is compacted away, so the index tracks what is
	// actually on disk.
	segSets   map[uint32]map[keyID]struct{}
	maxStore  int64
	threshold float64

	hits, misses, compactions atomic.Uint64

	compacting bool
	closed     bool
	compactWG  sync.WaitGroup

	done chan struct{}
	wg   sync.WaitGroup
}

// Open recovers the keydir from the on-disk segments and starts the background sync (everysec) and compaction loops.
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
		segSets:   make(map[uint32]map[keyID]struct{}),
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
	e.closed = true
	e.mu.Unlock()
	e.compactWG.Wait()
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.store.close()
}

func (e *Engine) recover() error {
	segs := e.store.segments()
	for i, seg := range segs {
		isLast := i == len(segs)-1
		// Later records win, so an overwrite or tombstone supersedes the earlier value. Only the newest segment may carry a
		// torn tail from a crash.
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

// setLoc records a live value's location and adds its live-byte accounting. Callers pass valLen rather than the value
// itself: recovery never needs the bytes, only their length.
func (e *Engine) setLoc(seg uint32, table, key string, valLen int, recPos, recSize int64) {
	e.noteSet(seg, table, key)
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

func (e *Engine) noteSet(seg uint32, table, key string) {
	id := hashKey(table, key)
	keys := e.segSets[seg]
	if keys == nil {
		keys = make(map[keyID]struct{})
		e.segSets[seg] = keys
	}
	keys[id] = struct{}{}
}

// buriedBefore reports whether a segment older than seg still holds a SET for id, which is what makes a tombstone in
// seg worth carrying forward. The index is consulted per tombstone rather than kept as a per-key hint: a hint would
// have to be repaired whenever the segment it names is reclaimed, and a hint that survives its segment silently keeps
// a delete alive forever.
func (e *Engine) buriedBefore(id keyID, seg uint32) bool {
	for other, keys := range e.segSets {
		if other >= seg {
			continue
		}
		if _, ok := keys[id]; ok {
			return true
		}
	}
	return false
}

// Set appends the value and updates the keydir and cache. It rejects the write with ErrStorageFull if the live dataset
// would exceed the configured limit.
func (e *Engine) Set(_ context.Context, tbl, key, value string) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	return e.setLocked(tbl, key, value)
}

// Update atomically replaces the value of key with what fn returns, reading the current value from the cache or its
// segment first. It holds the engine mutex across the whole read-modify-write, which is what makes INCR, APPEND and
// HSET atomic here. An error from fn appends nothing.
func (e *Engine) Update(_ context.Context, tbl, key string, fn func(old string, exists bool) (string, error)) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	var old string
	location, exists := e.lookup(tbl, key)
	if exists {
		var err error
		if old, err = e.value(tbl, key, location); err != nil {
			return err
		}
	}
	value, err := fn(old, exists)
	if err != nil {
		return err
	}
	return e.setLocked(tbl, key, value)
}

// setLocked appends a value and updates the keydir and cache. The caller holds e.mu.
func (e *Engine) setLocked(tbl, key, value string) error {
	if len(tbl) > maxFieldLen || len(key) > maxFieldLen {
		return fmt.Errorf("table/key exceeds %d bytes", maxFieldLen)
	}
	if len(value) > maxValueLen {
		return fmt.Errorf("value exceeds %d bytes", maxValueLen)
	}
	rec := encodeRecord(tbl, key, value, false)
	recSize := int64(len(rec))

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

// maxReadAttempts bounds the optimistic read loop. A retry means the record moved between the lookup and the read — a
// concurrent overwrite or a compaction pass — and after a few of those the read falls back to holding the engine
// exclusively, where nothing can move it. Without the bound a key rewritten in a tight loop could keep a reader
// spinning.
const maxReadAttempts = 3

// Get returns a value, populating the cache on a miss by reading from a pinned segment without holding the engine lock.
func (e *Engine) Get(_ context.Context, tbl, key string) (string, error) {
	for range maxReadAttempts {
		value, done, err := e.tryGet(tbl, key)
		if done {
			return value, err
		}
	}

	e.mu.Lock()
	defer e.mu.Unlock()
	location, ok := e.lookup(tbl, key)
	if !ok {
		return "", engine.ErrNotFound
	}
	return e.value(tbl, key, location)
}

// tryGet serves one optimistic attempt: it pins the value's segment under the read lock, reads it with the lock
// released, then keeps what it read only if the keydir still points at the same record. done is false when the record
// moved underneath it and the caller should look again.
func (e *Engine) tryGet(tbl, key string) (value string, done bool, err error) {
	e.mu.RLock()
	location, ok := e.lookup(tbl, key)
	if !ok {
		e.mu.RUnlock()
		return "", true, engine.ErrNotFound
	}
	if cached, hit := e.lru.get(tbl, key); hit {
		e.hits.Add(1)
		e.mu.RUnlock()
		return cached, true, nil
	}
	pinned, ok := e.store.pin(location.seg)
	e.mu.RUnlock()
	if !ok {
		return "", false, nil // segment reclaimed since the lookup
	}

	value, err = readPinnedValue(location.seg, pinned, location.valPos, location.valLen)
	e.store.unpin(location.seg)
	if err != nil {
		return "", true, err
	}

	// Re-check and cache under the read lock, not an exclusive one: the keydir is only read here and the cache carries its
	// own mutex, so concurrent cold reads do not queue up behind each other at the end of every miss.
	e.mu.RLock()
	defer e.mu.RUnlock()
	if current, exists := e.lookup(tbl, key); !exists || current != location {
		return "", false, nil
	}
	e.misses.Add(1)
	e.lru.put(tbl, key, value)
	return value, true, nil
}

func (e *Engine) value(tbl, key string, location loc) (string, error) {
	if value, hit := e.lru.get(tbl, key); hit {
		e.hits.Add(1)
		return value, nil
	}
	e.misses.Add(1)
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
	e.mu.RLock()
	defer e.mu.RUnlock()
	return slices.Sorted(maps.Keys(e.keydir))
}

// TableExists reports whether a table has at least one live key.
func (e *Engine) TableExists(_ context.Context, tbl string) bool {
	e.mu.RLock()
	defer e.mu.RUnlock()
	_, ok := e.keydir[tbl]
	return ok
}

// Keys returns all keys in a table in sorted order.
func (e *Engine) Keys(_ context.Context, tbl string) []string {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return slices.Sorted(maps.Keys(e.keydir[tbl]))
}

// Range calls fn for every live value, reading from disk on a cache miss.
func (e *Engine) Range(fn func(table, key, value string) bool) {
	e.mu.RLock()
	defer e.mu.RUnlock()
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

// Replace swaps all state for a replication resync snapshot — unreachable here: it is called only by a standby, and the
// config rejects tiered alongside replication. It stays a stub rather than untested machinery; wiring the two together
// needs a shared LSN-tagged log, not this method (see docs/plans/05).
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
